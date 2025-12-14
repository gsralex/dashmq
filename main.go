package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gsralex/dashmq/internal/config"
	"github.com/gsralex/dashmq/internal/server"
)

func main() {
	var (
		host     = flag.String("host", "0.0.0.0", "Server host")
		port     = flag.Int("port", 9092, "Server port")
		dataDir  = flag.String("data-dir", "./data", "Data directory")
		logLevel = flag.String("log-level", "info", "Log level")
	)
	flag.Parse()

	cfg := &config.Config{
		Host:     *host,
		Port:     *port,
		DataDir:  *dataDir,
		LogLevel: *logLevel,
	}

	srv := server.New(cfg)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v, shutting down...", sig)
		srv.Stop()
		os.Exit(0)
	}()

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	log.Printf("Starting DashMQ server on %s", addr)
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
