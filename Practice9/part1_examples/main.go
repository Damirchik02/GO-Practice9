package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

func doSomethingUnreliable() error {
	if rand.Intn(10) < 7 {
		fmt.Println("Operation failed, retrying...")
		return errors.New("temporary failure")
	}
	fmt.Println("Operation succeeded!")
	return nil
}

func example1InfiniteLoop() {
	fmt.Println("\nExample 1: Infinite Loop (anti-pattern)")
	var err error
	count := 0
	for count < 20 {
		err = doSomethingUnreliable()
		if err == nil {
			break
		}
		count++
	}
	if err != nil {
		fmt.Printf("Failed after %d tries: %v\n", count, err)
	}
}

func example2FixedDelay() {
	fmt.Println("\nExample 2: Fixed Delay Retry")
	var err error
	const maxRetries = 5
	const delay = 200 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = doSomethingUnreliable()
		if err == nil {
			break
		}
		fmt.Printf("Attempt %d failed, waiting %v before next retry...\n", attempt+1, delay)
		if attempt < maxRetries-1 {
			time.Sleep(delay)
		}
	}
	if err != nil {
		fmt.Printf("Failed after %d attempts: %v\n", maxRetries, err)
	} else {
		fmt.Println("Succeeded within retry limit.")
	}
}

func example3ExponentialBackoff() {
	fmt.Println("\nExample 3: Exponential Backoff")
	var err error
	const maxRetries = 5
	baseDelay := 100 * time.Millisecond
	maxDelay := 2 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = doSomethingUnreliable()
		if err == nil {
			break
		}
		if attempt == maxRetries-1 {
			break
		}
		backoffTime := baseDelay * time.Duration(math.Pow(2, float64(attempt)))
		if backoffTime > maxDelay {
			backoffTime = maxDelay
		}
		fmt.Printf("Attempt %d failed, waiting %v before next retry...\n", attempt+1, backoffTime)
		time.Sleep(backoffTime)
	}
	if err != nil {
		fmt.Printf("Failed after %d attempts: %v\n", maxRetries, err)
	} else {
		fmt.Println("Succeeded!")
	}
}

func example4BackoffWithJitter() {
	fmt.Println("\nExample 4: Exponential Backoff + Full Jitter")
	var err error
	const maxRetries = 5
	baseDelay := 100 * time.Millisecond
	maxDelay := 2 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		err = doSomethingUnreliable()
		if err == nil {
			break
		}
		if attempt == maxRetries-1 {
			break
		}
		backoffTime := baseDelay * time.Duration(math.Pow(2, float64(attempt)))
		if backoffTime > maxDelay {
			backoffTime = maxDelay
		}
		jitter := time.Duration(rand.Int63n(int64(backoffTime)))
		fmt.Printf("Attempt %d failed, waiting ~%v (max backoff: %v, full jitter applied)\n",
			attempt+1, jitter, backoffTime)
		time.Sleep(jitter)
	}
	if err != nil {
		fmt.Printf("Failed after %d attempts: %v\n", maxRetries, err)
	} else {
		fmt.Println("Succeeded!")
	}
}

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func doSomethingUnreliableCtx(ctx context.Context) error {
	if rand.Intn(10) < 7 {
		return errors.New("temporary failure")
	}
	return nil
}

func RetryWithContext(ctx context.Context, cfg RetryConfig) error {
	var err error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			fmt.Printf("Context cancelled: %v\n", ctx.Err())
			return ctx.Err()
		}

		err = doSomethingUnreliableCtx(ctx)
		if err == nil {
			fmt.Println("Operation succeeded!")
			return nil
		}

		if attempt == cfg.MaxRetries-1 {
			break
		}

		backoff := cfg.BaseDelay * time.Duration(math.Pow(2, float64(attempt)))
		if backoff > cfg.MaxDelay {
			backoff = cfg.MaxDelay
		}
		jitter := time.Duration(rand.Int63n(int64(backoff) + 1))

		fmt.Printf("Attempt %d failed, waiting ~%v...\n", attempt+1, jitter)

		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			fmt.Printf("Context cancelled during wait: %v\n", ctx.Err())
			return ctx.Err()
		}
	}
	return err
}

func example5Context() {
	fmt.Println("\nExample 5: Context + Cancellation")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg := RetryConfig{
		MaxRetries: 10,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   2 * time.Second,
	}

	err := RetryWithContext(ctx, cfg)
	if err != nil {
		fmt.Printf("Final error: %v\n", err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	example1InfiniteLoop()
	example2FixedDelay()
	example3ExponentialBackoff()
	example4BackoffWithJitter()
	example5Context()

	fmt.Println("\nAll Part 1 examples done")
}
