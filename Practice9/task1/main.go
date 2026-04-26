package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"
)

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func IsRetryable(resp *http.Response, err error) bool {
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			if urlErr.Timeout() {
				return true
			}
		}
		return true
	}

	if resp != nil {
		switch resp.StatusCode {
		case http.StatusTooManyRequests:
			return true
		case http.StatusInternalServerError:
			return true
		case http.StatusBadGateway:
			return true
		case http.StatusServiceUnavailable:
			return true
		case http.StatusGatewayTimeout:
			return true
		case http.StatusUnauthorized:
			return false
		case http.StatusNotFound:
			return false
		default:
			return false
		}
	}

	return false
}

func CalculateBackoff(attempt int, cfg RetryConfig) time.Duration {
	backoff := cfg.BaseDelay * time.Duration(math.Pow(2, float64(attempt)))

	if backoff > cfg.MaxDelay {
		backoff = cfg.MaxDelay
	}

	jitter := time.Duration(rand.Int63n(int64(backoff) + 1))
	return jitter
}

type PaymentClient struct {
	httpClient *http.Client
	cfg        RetryConfig
	gatewayURL string
}

func NewPaymentClient(gatewayURL string, cfg RetryConfig) *PaymentClient {
	return &PaymentClient{
		httpClient: &http.Client{Timeout: 5 * time.Second},
		cfg:        cfg,
		gatewayURL: gatewayURL,
	}
}

func (c *PaymentClient) ExecutePayment(ctx context.Context) (map[string]interface{}, error) {
	var lastErr error

	for attempt := 0; attempt < c.cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("context cancelled before attempt %d: %w", attempt+1, ctx.Err())
		}

		fmt.Printf("[Attempt %d] Sending payment request...\n", attempt+1)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.gatewayURL+"/pay", nil)
		if err != nil {
			return nil, fmt.Errorf("failed to build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)

		if !IsRetryable(resp, err) {
			if err != nil {
				return nil, fmt.Errorf("non-retriable error: %w", err)
			}
			if resp != nil && resp.StatusCode == http.StatusOK {
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				var result map[string]interface{}
				json.Unmarshal(body, &result)
				fmt.Printf("[Attempt %d] Success! Response: %v\n", attempt+1, result)
				return result, nil
			}
			if resp != nil {
				resp.Body.Close()
				return nil, fmt.Errorf("non-retriable HTTP status: %d", resp.StatusCode)
			}
		}

		if err != nil {
			lastErr = err
			fmt.Printf("[Attempt %d] Network error: %v\n", attempt+1, err)
		} else if resp != nil {
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			fmt.Printf("[Attempt %d] Failed with status: %d %s\n",
				attempt+1, resp.StatusCode, http.StatusText(resp.StatusCode))
			resp.Body.Close()
		}

		if attempt == c.cfg.MaxRetries-1 {
			break
		}

		waitDuration := CalculateBackoff(attempt, c.cfg)
		fmt.Printf("[Attempt %d] Waiting ~%v before next retry...\n", attempt+1, waitDuration.Round(time.Millisecond))

		select {
		case <-time.After(waitDuration):
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled while waiting: %w", ctx.Err())
		}
	}

	return nil, fmt.Errorf("failed after %d attempts: %w", c.cfg.MaxRetries, lastErr)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("Task 1: Resilient HTTP Client (Payment GW)")
	fmt.Println()

	requestCount := 0

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		fmt.Printf("[Server] Received request #%d\n", requestCount)

		if requestCount <= 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `{"error":"service temporarily unavailable"}`)
			fmt.Printf("[Server] Responded with 503 (simulating overload)\n")
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"success","payment_id":"pay-abc-123","amount":1000}`)
			fmt.Printf("[Server] Responded with 200 OK (recovered!)\n")
		}
	}))
	defer testServer.Close()

	cfg := RetryConfig{
		MaxRetries: 5,
		BaseDelay:  500 * time.Millisecond,
		MaxDelay:   10 * time.Second,
	}

	client := NewPaymentClient(testServer.URL, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("Config: MaxRetries=%d, BaseDelay=%v, MaxDelay=%v\n\n",
		cfg.MaxRetries, cfg.BaseDelay, cfg.MaxDelay)

	start := time.Now()
	result, err := client.ExecutePayment(ctx)
	elapsed := time.Since(start)

	fmt.Println()
	if err != nil {
		fmt.Printf(" Payment failed: %v\n", err)
	} else {
		fmt.Printf(" Payment succeeded in %v!\n", elapsed.Round(time.Millisecond))
		fmt.Printf("   Result: %v\n", result)
	}

	fmt.Println()
	fmt.Println("Task 1 done")
}
