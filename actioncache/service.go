// Copyright 2023 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package actioncache implements the REAPI ActionCache service.
package actioncache

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	remote "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/philwo/kajiya/blobstore"
)

// Service implements the REAPI ActionCache service.
type Service struct {
	remote.UnimplementedActionCacheServer

	// The ActionCache to use for storing ActionResults.
	ac *ActionCache

	// The blobstore.ContentAddressableStorage to use for reading blobs.
	cas *blobstore.ContentAddressableStorage
}

// Register creates and registers a new Service with the given gRPC server.
func Register(s *grpc.Server, ac *ActionCache, cas *blobstore.ContentAddressableStorage) error {
	service, err := NewService(ac, cas)
	if err != nil {
		return err
	}
	remote.RegisterActionCacheServer(s, service)
	return nil
}

// NewService creates a new Service.
func NewService(ac *ActionCache, cas *blobstore.ContentAddressableStorage) (Service, error) {
	if ac == nil {
		return Service{}, fmt.Errorf("ac must be set")
	}

	if cas == nil {
		return Service{}, fmt.Errorf("cas must be set")
	}

	return Service{
		ac:  ac,
		cas: cas,
	}, nil
}

// GetActionResult returns the ActionResult for a given action digest.
func (s Service) GetActionResult(ctx context.Context, request *remote.GetActionResultRequest) (*remote.ActionResult, error) {
	response, err := s.getActionResult(request)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			log.Printf("⚠️ GetActionResult(%v) => Cache miss", request.ActionDigest)
		} else {
			log.Printf("🚨 GetActionResult(%v) => Error: %v", request.ActionDigest, err)
		}
	} else {
		log.Printf("🎉 GetActionResult(%v) => Cache hit", request.ActionDigest)
	}
	return response, err
}

func (s Service) getActionResult(request *remote.GetActionResultRequest) (*remote.ActionResult, error) {
	// If the client explicitly specifies a DigestFunction, ensure that it's SHA256.
	if request.DigestFunction != remote.DigestFunction_UNKNOWN && request.DigestFunction != remote.DigestFunction_SHA256 {
		return nil, status.Errorf(codes.InvalidArgument, "hash function %q is not supported", request.DigestFunction.String())
	}

	actionDigest, err := digest.NewFromProto(request.ActionDigest)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	actionResult, err := s.ac.Get(actionDigest)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "action digest %s not found in cache", actionDigest)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return actionResult, nil
}

// UpdateActionResult stores an ActionResult for a given action digest on disk.
func (s Service) UpdateActionResult(ctx context.Context, request *remote.UpdateActionResultRequest) (*remote.ActionResult, error) {
	response, err := s.updateActionResult(request)
	if err != nil {
		log.Printf("🚨 UpdateActionResult(%v) => Error: %v", request.ActionDigest, err)
	} else {
		log.Printf("✅ UpdateActionResult(%v) => OK", request.ActionDigest)
	}
	return response, err
}

func (s Service) updateActionResult(request *remote.UpdateActionResultRequest) (*remote.ActionResult, error) {
	// If the client explicitly specifies a DigestFunction, ensure that it's SHA256.
	if request.DigestFunction != remote.DigestFunction_UNKNOWN && request.DigestFunction != remote.DigestFunction_SHA256 {
		return nil, status.Errorf(codes.InvalidArgument, "hash function %q is not supported", request.DigestFunction.String())
	}

	// Check that the client didn't send inline stdout / stderr data.
	if request.ActionResult.StdoutRaw != nil {
		return nil, status.Error(codes.InvalidArgument, "client should not populate stdout_raw during upload")
	}
	if request.ActionResult.StderrRaw != nil {
		return nil, status.Error(codes.InvalidArgument, "client should not populate stderr_raw during upload")
	}

	// Check that the action digest is valid.
	actionDigest, err := digest.NewFromProto(request.ActionDigest)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Check that the action is present in our CAS.
	if _, err := s.cas.Stat(actionDigest); err != nil {
		return nil, status.Errorf(codes.NotFound, "action digest %s not found in CAS", actionDigest)
	}

	// If the action result contains a stdout digest, check that it is present in our CAS.
	if request.ActionResult.StdoutDigest != nil {
		stdoutDigest, err := digest.NewFromProto(request.ActionResult.StdoutDigest)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if _, err := s.cas.Stat(stdoutDigest); err != nil {
			return nil, status.Errorf(codes.NotFound, "stdout digest %s not found in CAS", stdoutDigest)
		}
	}

	// Same for stderr.
	if request.ActionResult.StderrDigest != nil {
		stderrDigest, err := digest.NewFromProto(request.ActionResult.StderrDigest)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if _, err := s.cas.Stat(stderrDigest); err != nil {
			return nil, status.Errorf(codes.NotFound, "stderr digest %s not found in CAS", stderrDigest)
		}
	}

	// TODO: Check that all the output files are present in our CAS.

	// Store the action result.
	if err := s.ac.Put(actionDigest, request.ActionResult); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Return the action result.
	return request.ActionResult, nil
}
