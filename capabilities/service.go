// Copyright 2023 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package capabilities implements the REAPI Capabilities service.
package capabilities

import (
	"context"
	"log"

	remote "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"google.golang.org/grpc"
)

// Service implements the REAPI Capabilities service.
type Service struct {
	remote.UnimplementedCapabilitiesServer
}

// Register creates and registers a new Service with the given gRPC server.
func Register(s *grpc.Server) {
	remote.RegisterCapabilitiesServer(s, NewService())
}

// NewService creates a new Service.
func NewService() *Service {
	return &Service{}
}

// GetCapabilities returns the capabilities of the server.
func (s *Service) GetCapabilities(ctx context.Context, request *remote.GetCapabilitiesRequest) (*remote.ServerCapabilities, error) {
	response, err := s.getCapabilities(request)
	if err != nil {
		log.Printf("⚠️ GetCapabilities(%v) => Error: %v", request, err)
	} else {
		log.Printf("✅ GetCapabilities(%v) => OK", request)
	}
	return response, err
}

func (s *Service) getCapabilities(request *remote.GetCapabilitiesRequest) (*remote.ServerCapabilities, error) {
	// Return the capabilities.
	return &remote.ServerCapabilities{
		CacheCapabilities: &remote.CacheCapabilities{
			DigestFunctions: []remote.DigestFunction_Value{
				remote.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &remote.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			CachePriorityCapabilities: &remote.PriorityCapabilities{
				Priorities: []*remote.PriorityCapabilities_PriorityRange{
					{
						MinPriority: 0,
						MaxPriority: 0,
					},
				},
			},
			MaxBatchTotalSizeBytes:      0,                                             // no limit.
			SymlinkAbsolutePathStrategy: remote.SymlinkAbsolutePathStrategy_DISALLOWED, // Same as RBE.
		},
		ExecutionCapabilities: &remote.ExecutionCapabilities{
			DigestFunction: remote.DigestFunction_SHA256,
			DigestFunctions: []remote.DigestFunction_Value{
				remote.DigestFunction_SHA256,
			},
			ExecEnabled: true,
			ExecutionPriorityCapabilities: &remote.PriorityCapabilities{
				Priorities: []*remote.PriorityCapabilities_PriorityRange{
					{
						MinPriority: 0,
						MaxPriority: 0,
					},
				},
			},
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 0}, // RBE does not support higher versions, so we don't either.
	}, nil
}
