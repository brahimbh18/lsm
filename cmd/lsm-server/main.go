package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"lsm/internal/engine"
	"lsm/internal/server"
)

func main() {
	var (
		addr    = flag.String("addr", ":8080", "HTTP server listen address")
		dataDir = flag.String("data-dir", "./data", "database data directory")
	)
	flag.Parse()

	var debugLogger engine.DebugLogger
	var debugLevel int
	if val := os.Getenv("LSM_DEBUG"); val != "" {
		if lvl, err := strconv.Atoi(val); err == nil && lvl > 0 {
			debugLogger = log.Default()
			debugLevel = lvl
		}
	}

	walDir := envOrDefault("WAL_DIR", *dataDir)
	tablesDir := envOrDefault("DATA_DIR", filepath.Join(*dataDir, "tables"))

	db, err := engine.Open(engine.Options{
		WALDir:      walDir,
		DataDir:     tablesDir,
		SyncMode:    engine.SyncModeSync,
		DebugLogger: debugLogger,
		DebugLevel:  debugLevel,
	})
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	var serverOpts []server.Option
	if debugLogger != nil {
		serverOpts = append(serverOpts, server.WithDebug(debugLogger, debugLevel))
	}
	srv := server.New(db, serverOpts...)

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: srv,
	}

	shutdownDone := make(chan struct{})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
		}
		close(shutdownDone)
	}()

	fmt.Printf("LSM server listening on %s\n", *addr)
	fmt.Printf("data directory: %s\n", *dataDir)

	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}

	<-shutdownDone

	if err := db.Close(); err != nil {
		log.Fatalf("error closing database: %v", err)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
