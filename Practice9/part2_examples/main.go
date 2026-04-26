package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

type CachedResponse struct {
	StatusCode int
	Body       []byte
	Completed  bool
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
	m.data[key] = &CachedResponse{Completed: false}
	return true
}

func (m *MemoryStore) Finish(key string, status int, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if resp, exists := m.data[key]; exists {
		resp.StatusCode = status
		resp.Body = body
		resp.Completed = true
	} else {
		m.data[key] = &CachedResponse{StatusCode: status, Body: body, Completed: true}
	}
}

func IdempotencyMiddleware(store *MemoryStore, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			http.Error(w, "Idempotency-Key header required", http.StatusBadRequest)
			return
		}

		if cached, exists := store.Get(key); exists {
			if cached.Completed {
				fmt.Printf("[Middleware] Key %s already completed — returning cached response\n", key)
				w.WriteHeader(cached.StatusCode)
				w.Write(cached.Body)
			} else {
				fmt.Printf("[Middleware] Key %s is still processing — 409 Conflict\n", key)
				http.Error(w, "Duplicate request in progress", http.StatusConflict)
			}
			return
		}

		if !store.StartProcessing(key) {
			if cached, exists := store.Get(key); exists && cached.Completed {
				w.WriteHeader(cached.StatusCode)
				w.Write(cached.Body)
			} else {
				http.Error(w, "Duplicate request in progress", http.StatusConflict)
			}
			return
		}

		fmt.Printf("[Middleware] Key %s is new — processing...\n", key)

		recorder := httptest.NewRecorder()
		next.ServeHTTP(recorder, r)

		store.Finish(key, recorder.Code, recorder.Body.Bytes())

		for k, vals := range recorder.Header() {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(recorder.Code)
		w.Write(recorder.Body.Bytes())
	})
}

func exampleHandler(w http.ResponseWriter, r *http.Request) {
	time.Sleep(100 * time.Millisecond)
	txID := fmt.Sprintf("tx-%d", rand.Intn(100000))
	resp := map[string]interface{}{
		"status":         "processed",
		"transaction_id": txID,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
	fmt.Printf("[Handler] Business logic executed! tx_id=%s\n", txID)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	fmt.Println("Part 2 Example: Idempotency Middleware")

	store := NewMemoryStore()
	mux := http.NewServeMux()
	mux.HandleFunc("/process", exampleHandler)

	server := httptest.NewServer(IdempotencyMiddleware(store, mux))
	defer server.Close()

	idempotencyKey := "test-key-abc-123"

	sendRequest := func(label string) {
		req, _ := http.NewRequest("POST", server.URL+"/process", nil)
		req.Header.Set("Idempotency-Key", idempotencyKey)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("[%s] Error: %v\n", label, err)
			return
		}
		defer resp.Body.Close()

		var body map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&body)
		fmt.Printf("[%s] Status: %d, Body: %v\n", label, resp.StatusCode, body)
	}

	fmt.Println("--- Request 1 (new key) ---")
	sendRequest("Request-1")

	fmt.Println("\n--- Request 2 (duplicate key) ---")
	sendRequest("Request-2")

	fmt.Println("\n--- Request 3 (missing key) ---")
	req, _ := http.NewRequest("POST", server.URL+"/process", nil)
	client := &http.Client{}
	resp, _ := client.Do(req)
	fmt.Printf("[Request-3] Status: %d (expected 400)\n", resp.StatusCode)

	fmt.Println("\nPart 2 example done")
}
