package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"lsm/internal/engine"
	"lsm/internal/server"
)

func main() {
	addr := flag.String("addr", ":6380", "RESP2 server listen address")
	dataDir := flag.String("data-dir", "./data", "database data directory")
	walSync := flag.String("wal-sync", "always", "WAL sync policy: always, everysec, or never")
	walSyncInterval := flag.Duration("wal-sync-interval", time.Second, "WAL sync interval for everysec")
	flag.Parse()
	syncPolicy, err := engine.ParseSyncPolicy(*walSync)
	if err != nil {
		log.Fatal(err)
	}

	var debugLogger engine.DebugLogger
	var debugLevel int
	if value := os.Getenv("LSM_DEBUG"); value != "" {
		if level, err := strconv.Atoi(value); err == nil && level > 0 {
			debugLogger, debugLevel = log.Default(), level
		}
	}

	db, err := engine.Open(engine.Options{
		DataDir:    envOrDefault("DATA_DIR", *dataDir),
		SyncPolicy: syncPolicy, SyncInterval: *walSyncInterval,
		DebugLogger: debugLogger, DebugLevel: debugLevel,
	})
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("error closing database: %v", err)
		}
	}()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	srv := server.New(db, server.DefaultConfig)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdown
		_ = listener.Close()
	}()

	log.Printf("LSM RESP2 server listening on %s", *addr)
	log.Printf("data directory: %s", envOrDefault("DATA_DIR", *dataDir))
	log.Printf("wal sync policy=%s interval=%s", syncPolicy, *walSyncInterval)
	if err := srv.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
