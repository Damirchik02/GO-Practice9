package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"
)

func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

const (
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
)

type CachedResponse struct {
	Status     string
	StatusCode int
	Body       []byte
}

type MemoryStore struct {
	mu   sync.Mutex
	data map[string]*CachedResponse
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string]*CachedResponse)}
}

func (m *MemoryStore) Get(key string) (*CachedResponse, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	resp, exists := m.data[key]
	return resp, exists
}

func (m *MemoryStore) StartProcessing(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[key]; exists {
		return false
	}
	m.data[key] = &CachedResponse{Status: StatusProcessing}
	return true
}

func (m *MemoryStore) Finish(key string, statusCode int, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = &CachedResponse{
		Status:     StatusCompleted,
		StatusCode: statusCode,
		Body:       body,
	}
}

func IdempotencyMiddleware(store *MemoryStore, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			http.Error(w, `{"error":"Idempotency-Key header is required"}`, http.StatusBadRequest)
			fmt.Println("[Middleware] Missing Idempotency-Key → 400 Bad Request")
			return
		}

		if cached, exists := store.Get(key); exists {
			switch cached.Status {
			case StatusProcessing:
				fmt.Printf("[Middleware] Key %s is still processing → 409 Conflict\n", key)
				http.Error(w, `{"error":"Duplicate request in progress"}`, http.StatusConflict)
				return
			case StatusCompleted:
				fmt.Printf("[Middleware] Key %s already completed → returning cached response\n", key)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Idempotent-Replayed", "true")
				w.WriteHeader(cached.StatusCode)
				w.Write(cached.Body)
				return
			}
		}

		if !store.StartProcessing(key) {
			if cached, exists := store.Get(key); exists && cached.Status == StatusCompleted {
				fmt.Printf("[Middleware] Key %s just completed (race) → returning cached response\n", key)
				w.WriteHeader(cached.StatusCode)
				w.Write(cached.Body)
			} else {
				fmt.Printf("[Middleware] Key %s taken by race → 409 Conflict\n", key)
				http.Error(w, `{"error":"Duplicate request in progress"}`, http.StatusConflict)
			}
			return
		}

		fmt.Printf("[Middleware] Key %s is new → executing business logic\n", key)

		recorder := httptest.NewRecorder()
		next.ServeHTTP(recorder, r)

		store.Finish(key, recorder.Code, recorder.Body.Bytes())
		fmt.Printf("[Middleware] Key %s result saved (status %d)\n", key, recorder.Code)

		for k, vals := range recorder.Header() {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(recorder.Code)
		w.Write(recorder.Body.Bytes())
	})
}

func paymentHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("[Handler] Payment business logic started...")

	time.Sleep(2 * time.Second)

	txID := newUUID()

	response := map[string]interface{}{
		"status":         "paid",
		"amount":         1000,
		"transaction_id": txID,
	}

	fmt.Printf("[Handler] Payment completed! tx_id=%s\n", txID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func main() {
	fmt.Println("Task 2: Loan Repayment — Idempotency")
	fmt.Println()

	store := NewMemoryStore()
	mux := http.NewServeMux()
	mux.HandleFunc("/pay", paymentHandler)

	server := httptest.NewServer(IdempotencyMiddleware(store, mux))
	defer server.Close()

	sendPayment := func(workerID int, idempotencyKey string, results chan<- string) {
		client := &http.Client{Timeout: 10 * time.Second}
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/pay", strings.NewReader(`{"amount":1000}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			results <- fmt.Sprintf("Worker %d: ERROR %v", workerID, err)
			return
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		replayed := resp.Header.Get("X-Idempotent-Replayed")
		if replayed == "true" {
			results <- fmt.Sprintf("Worker %d: Status=%d (CACHED REPLAY) Body=%s",
				workerID, resp.StatusCode, strings.TrimSpace(string(body)))
		} else {
			results <- fmt.Sprintf("Worker %d: Status=%d Body=%s",
				workerID, resp.StatusCode, strings.TrimSpace(string(body)))
		}
	}

	fmt.Println("─── Scenario 1: Missing Idempotency-Key ───")
	resp, _ := http.Post(server.URL+"/pay", "application/json", nil)
	fmt.Printf("Response: %d (expected 400 Bad Request)\n\n", resp.StatusCode)

	fmt.Println("─── Scenario 2: 5 simultaneous requests with the SAME key ───")
	fmt.Println("(Simulating user clicking 'Pay' 5 times rapidly)")
	fmt.Println()

	sharedKey := "idempotency-key-" + newUUID()
	fmt.Printf("Using Idempotency-Key: %s\n\n", sharedKey)

	const numWorkers = 5
	results := make(chan string, numWorkers)
	var wg sync.WaitGroup

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sendPayment(id, sharedKey, results)
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	fmt.Println("Results from concurrent requests:")
	for result := range results {
		fmt.Printf("  → %s\n", result)
	}

	fmt.Println()
	fmt.Println("─── Scenario 3: Replay with same key AFTER completion ───")
	fmt.Println("(Simulating client retry after network loss)")

	replayResults := make(chan string, 1)
	sendPayment(99, sharedKey, replayResults)
	fmt.Printf("  → %s\n", <-replayResults)
	fmt.Println("  ^ Notice: business logic NOT re-executed (tx_id is the same as the first request)")

	fmt.Println()
	fmt.Println("─── Scenario 4: New key → fresh payment ───")
	newKey := "idempotency-key-" + newUUID()
	fmt.Printf("Using new Idempotency-Key: %s\n", newKey)
	newResults := make(chan string, 1)
	sendPayment(100, newKey, newResults)
	fmt.Printf("  → %s\n", <-newResults)

	fmt.Println()
	fmt.Println("Task 2 done")
}
