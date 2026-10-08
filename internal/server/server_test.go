package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"lsm/internal/engine"
)

func setupTestDB(t *testing.T) *engine.DB {
	t.Helper()
	dir := t.TempDir()
	opts := engine.Options{
		WALDir:          filepath.Join(dir, "wal"),
		DataDir:         filepath.Join(dir, "tables"),
		MemTableMaxSize: engine.DefaultMemTableMaxSize,
		SyncMode:        engine.SyncModeSync,
	}
	db, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestHealth(t *testing.T) {
	db := setupTestDB(t)
	srv := New(db)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status 'ok', got %q", resp.Status)
	}
}

func TestPut(t *testing.T) {
	db := setupTestDB(t)
	srv := New(db)

	body := []byte(`{"value":"hello"}`)
	req := httptest.NewRequest(http.MethodPut, "/kv/foo", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var resp putResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if resp.Key != "foo" {
		t.Fatalf("expected key 'foo', got %q", resp.Key)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status 'ok', got %q", resp.Status)
	}
}

func TestPutAndGet(t *testing.T) {
	db := setupTestDB(t)
	srv := New(db)

	// PUT /kv/foo
	putBody := []byte(`{"value":"hello"}`)
	putReq := httptest.NewRequest(http.MethodPut, "/kv/foo", bytes.NewReader(putBody))
	putReq.Header.Set("Content-Type", "application/json")
	putRec := httptest.NewRecorder()

	srv.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PUT /kv/foo failed with status %d: %s", putRec.Code, putRec.Body.String())
	}

	// GET /kv/foo
	getReq := httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	getRec := httptest.NewRecorder()

	srv.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET /kv/foo failed with status %d: %s", getRec.Code, getRec.Body.String())
	}

	var resp getResponse
	if err := json.Unmarshal(getRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse GET response: %v", err)
	}
	if resp.Key != "foo" {
		t.Fatalf("expected key 'foo', got %q", resp.Key)
	}
	if resp.Value != "hello" {
		t.Fatalf("expected value 'hello', got %q", resp.Value)
	}
}

func TestMissingKey(t *testing.T) {
	db := setupTestDB(t)
	srv := New(db)

	req := httptest.NewRequest(http.MethodGet, "/kv/does-not-exist", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}

	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if resp.Error != "key not found" {
		t.Fatalf("expected error 'key not found', got %q", resp.Error)
	}
}

func TestMultipleKeys(t *testing.T) {
	db := setupTestDB(t)
	srv := New(db)

	entries := []struct {
		key   string
		value string
	}{
		{"foo", "hello"},
		{"bar", "world"},
		{"baz", "test"},
	}

	// PUT all keys
	for _, entry := range entries {
		body := []byte(fmt.Sprintf(`{"value":%q}`, entry.value))
		req := httptest.NewRequest(http.MethodPut, "/kv/"+entry.key, bytes.NewReader(body))
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("PUT /kv/%s failed with %d: %s", entry.key, rec.Code, rec.Body.String())
		}
	}

	// GET each key and verify they remain independent
	for _, entry := range entries {
		req := httptest.NewRequest(http.MethodGet, "/kv/"+entry.key, nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /kv/%s failed with %d: %s", entry.key, rec.Code, rec.Body.String())
		}

		var resp getResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response for %s: %v", entry.key, err)
		}
		if resp.Key != entry.key {
			t.Fatalf("expected key %q, got %q", entry.key, resp.Key)
		}
		if resp.Value != entry.value {
			t.Fatalf("expected value %q, got %q", entry.value, resp.Value)
		}
	}
}

func TestErrorHandling_MalformedRequests(t *testing.T) {
	db := setupTestDB(t)
	srv := New(db)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "invalid JSON syntax",
			method:     http.MethodPut,
			path:       "/kv/foo",
			body:       `{invalid json`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty request body",
			method:     http.MethodPut,
			path:       "/kv/foo",
			body:       "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing value field",
			method:     http.MethodPut,
			path:       "/kv/foo",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-string value field",
			method:     http.MethodPut,
			path:       "/kv/foo",
			body:       `{"value":123}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewBufferString(tc.body))
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tc.wantStatus, rec.Code, rec.Body.String())
			}

			var resp errorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("response is not valid error JSON: %v", err)
			}
			if resp.Error == "" {
				t.Fatal("expected non-empty error message")
			}
		})
	}
}

func TestErrorHandling_DBClosed(t *testing.T) {
	db := setupTestDB(t)
	srv := New(db)

	// Close the DB to simulate underlying DB failure
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close DB: %v", err)
	}

	putBody := []byte(`{"value":"hello"}`)
	req := httptest.NewRequest(http.MethodPut, "/kv/foo", bytes.NewReader(putBody))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	// Should return 503 Service Unavailable or 500
	if rec.Code != http.StatusServiceUnavailable && rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 503 or 500 status for closed DB, got %d", rec.Code)
	}

	var resp errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if resp.Error == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestEmptyValue(t *testing.T) {
	db := setupTestDB(t)
	srv := New(db)

	putBody := []byte(`{"value":""}`)
	putReq := httptest.NewRequest(http.MethodPut, "/kv/emptykey", bytes.NewReader(putBody))
	putRec := httptest.NewRecorder()
	srv.ServeHTTP(putRec, putReq)

	if putRec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", putRec.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/kv/emptykey", nil)
	getRec := httptest.NewRecorder()
	srv.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", getRec.Code)
	}

	var resp getResponse
	if err := json.Unmarshal(getRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Value != "" {
		t.Fatalf("expected empty value, got %q", resp.Value)
	}
}

func TestConcurrentClients(t *testing.T) {
	db := setupTestDB(t)
	srv := New(db)

	const numWorkers = 16
	const opsPerWorker = 50

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for worker := 0; worker < numWorkers; worker++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("w%d_k%d", workerID, i)
				val := fmt.Sprintf("v_%d_%d", workerID, i)

				// PUT
				putBody := []byte(fmt.Sprintf(`{"value":%q}`, val))
				putReq := httptest.NewRequest(http.MethodPut, "/kv/"+key, bytes.NewReader(putBody))
				putRec := httptest.NewRecorder()
				srv.ServeHTTP(putRec, putReq)
				if putRec.Code != http.StatusOK {
					t.Errorf("worker %d: PUT %s failed: %d", workerID, key, putRec.Code)
					return
				}

				// GET
				getReq := httptest.NewRequest(http.MethodGet, "/kv/"+key, nil)
				getRec := httptest.NewRecorder()
				srv.ServeHTTP(getRec, getReq)
				if getRec.Code != http.StatusOK {
					t.Errorf("worker %d: GET %s failed: %d", workerID, key, getRec.Code)
					return
				}

				var resp getResponse
				if err := json.Unmarshal(getRec.Body.Bytes(), &resp); err != nil {
					t.Errorf("worker %d: failed to parse GET: %v", workerID, err)
					return
				}
				if resp.Value != val {
					t.Errorf("worker %d: expected %s, got %s", workerID, val, resp.Value)
					return
				}
			}
		}(worker)
	}

	wg.Wait()
}
