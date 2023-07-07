// Copyright 2023 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Kajiya is an RBE-compatible REAPI backend implementation used as a testing
// server during development of Chromium's new build tooling. It is not meant
// for production use, but can be very useful for local testing of any remote
// execution related code.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/philwo/kajiya/actioncache"
	"github.com/philwo/kajiya/blobstore"
	"github.com/philwo/kajiya/capabilities"
	"github.com/philwo/kajiya/execution"
)

var (
	dataDir         = flag.String("dir", getDefaultDataDir(), "the directory to store our data in")
	listen          = flag.String("listen", "localhost:50051", "the address to listen on (e.g. localhost:50051 or unix:///tmp/kajiya.sock)")
	enableCache     = flag.Bool("cache", true, "whether to enable the action cache service")
	enableExecution = flag.Bool("execution", true, "whether to enable the execution service")
)

func getDefaultDataDir() string {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(cacheDir, "kajiya")
}

func main() {
	flag.Parse()

	// Ensure our data directory exists.
	if *dataDir == "" {
		log.Fatalf("no data directory specified")
	}

	log.Printf("💾 using data directory: %v", *dataDir)

	// Listen on the specified address.
	network, addr := parseAddress(*listen)
	listener, err := net.Listen(network, addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("🛜 listening on %v", listener.Addr())

	// Create the gRPC server and register the services.
	grpcServer, err := createServer(*dataDir)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	// Handle interrupts gracefully.
	HandleInterrupt(func() {
		grpcServer.GracefulStop()
	})

	// Start serving.
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// parseAddress parses the listen address from the command line flag.
// The address can be a TCP address (e.g. localhost:50051) or a Unix domain socket (e.g. unix:///tmp/kajiya.sock).
func parseAddress(addr string) (string, string) {
	network := "tcp"
	if strings.HasPrefix(addr, "unix://") {
		network = "unix"
		addr = addr[len("unix://"):]
	}
	return network, addr
}

// createServer creates a new gRPC server and registers the services.
func createServer(dataDir string) (*grpc.Server, error) {
	s := grpc.NewServer()

	capabilities.Register(s)
	log.Printf("✅ capabilities service")

	// Create a CAS backed by a local filesystem.
	casDir := filepath.Join(dataDir, "cas")
	cas, err := blobstore.New(casDir)
	if err != nil {
		return nil, err
	}

	// CAS service.
	uploadDir := filepath.Join(casDir, "tmp")
	err = blobstore.Register(s, cas, uploadDir)
	if err != nil {
		return nil, err
	}
	log.Printf("✅ content-addressable storage service")

	// Action cache service.
	var ac *actioncache.ActionCache
	if *enableCache {
		acDir := filepath.Join(dataDir, "ac")
		ac, err = actioncache.New(acDir)
		if err != nil {
			return nil, err
		}

		err = actioncache.Register(s, ac, cas)
		if err != nil {
			return nil, err
		}
		log.Printf("✅ action cache service")
	} else {
		log.Printf("⚠️ action cache service disabled")
	}

	// Execution service.
	if *enableExecution {
		execDir := filepath.Join(dataDir, "exec")
		executor, err := execution.New(execDir, cas)
		if err != nil {
			return nil, err
		}

		err = execution.Register(s, executor, ac, cas)
		if err != nil {
			return nil, err
		}
		log.Printf("✅ execution service")
	} else {
		log.Printf("⚠️ execution service disabled")
	}

	// Register the reflection service provided by gRPC.
	reflection.Register(s)
	log.Printf("✅ gRPC reflection service")

	return s, nil
}

// HandleInterrupt calls 'fn' in a separate goroutine on SIGTERM or Ctrl+C.
//
// When SIGTERM or Ctrl+C comes for a second time, logs to stderr and kills
// the process immediately via os.Exit(1).
//
// Returns a callback that can be used to remove the installed signal handlers.
func HandleInterrupt(fn func()) (stopper func()) {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		handled := false
		for range ch {
			if handled {
				log.Printf("🚨 received second interrupt signal, exiting now")
				os.Exit(1)
			}
			log.Printf("⚠️ received signal, attempting graceful shutdown")
			handled = true
			go fn()
		}
	}()
	return func() {
		signal.Stop(ch)
		close(ch)
	}
}
