package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"lsm/internal/engine"
)

// responseWriter is a wrapper around http.ResponseWriter that captures the status code for logging.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// Store is the persistence boundary required by the HTTP adapter.
type Store interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool)
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Server provides an HTTP interface on top of a Store.
type Server struct {
	store       Store
	mux         *http.ServeMux
	debugLogger engine.DebugLogger
	debugLevel  int
}

// Option configures Server options.
type Option func(*Server)

// WithDebug configures a debug logger and level for the server.
func WithDebug(logger engine.DebugLogger, level int) Option {
	return func(s *Server) {
		s.debugLogger = logger
		s.debugLevel = level
	}
}

// New creates a new HTTP Server wrapping the provided Store.
func New(db Store, opts ...Option) *Server {
	s := &Server{
		store: db,
		mux:   http.NewServeMux(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /kv/{key}", s.handleGet)
	s.mux.HandleFunc("PUT /kv/{key}", s.handlePut)
}

// Handler returns the underlying http.Handler.
func (s *Server) Handler() http.Handler {
	return s.mux
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.debugLogger != nil && s.debugLevel >= 1 {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		s.mux.ServeHTTP(rw, r)
		s.debugLogger.Printf("[HTTP] method=%s path=%s status=%d duration=%s", r.Method, r.URL.Path, rw.statusCode, time.Since(start))
		return
	}
	s.mux.ServeHTTP(w, r)
}

type healthResponse struct {
	Status string `json:"status"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

type putRequest struct {
	Value *string `json:"value"`
}

type putResponse struct {
	Key    string `json:"key"`
	Status string `json:"status"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	// Limit request body size to 16 MiB
	r.Body = http.MaxBytesReader(w, r.Body, 16*1024*1024)

	var req putRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		if errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "request body is empty")
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}

	if req.Value == nil {
		writeError(w, http.StatusBadRequest, "value field is required")
		return
	}

	if err := s.store.Put([]byte(key), []byte(*req.Value)); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, engine.ErrClosed) {
			status = http.StatusServiceUnavailable
		}
		writeError(w, status, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, putResponse{
		Key:    key,
		Status: "ok",
	})
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	value, ok := s.store.Get([]byte(key))
	if !ok {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}

	writeJSON(w, http.StatusOK, getResponse{
		Key:   key,
		Value: string(value),
	})
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
