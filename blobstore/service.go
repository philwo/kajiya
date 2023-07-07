// Copyright 2023 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package cas implements the REAPI ContentAddressableStorage and ByteStream services.
package blobstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	remote "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// The maximum chunk size to write back to the client in Send calls.
	maxChunkSize int64 = 2 * 1024 * 1024 // 2M
)

// Service implements the REAPI ContentAddressableStorage and ByteStream services.
type Service struct {
	remote.UnimplementedContentAddressableStorageServer
	bytestream.UnimplementedByteStreamServer

	cas       *ContentAddressableStorage
	uploadDir string
}

// Register creates and registers a new Service with the given gRPC server.
// The dataDir is created if it does not exist.
func Register(s *grpc.Server, cas *ContentAddressableStorage, dataDir string) error {
	service, err := NewService(cas, dataDir)
	if err != nil {
		return err
	}
	bytestream.RegisterByteStreamServer(s, service)
	remote.RegisterContentAddressableStorageServer(s, service)
	return nil
}

// NewService creates a new Service.
func NewService(cas *ContentAddressableStorage, uploadDir string) (*Service, error) {
	if uploadDir == "" {
		return nil, errors.New("uploadDir must be set")
	}

	// Ensure that our temporary upload directory exists.
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return nil, err
	}

	return &Service{
		cas:       cas,
		uploadDir: uploadDir,
	}, nil
}

// parseReadResource parses a ReadRequest.ResourceName and returns the validated Digest.
// The resource name should be of the format: {instance_name}/blobs/{hash}/{size}
func parseReadResource(name string) (d digest.Digest, err error) {
	fields := strings.Split(name, "/")

	// Strip any parts before "blobs", as they'll belong to an instance name.
	for i := range fields {
		if fields[i] == "blobs" {
			fields = fields[i:]
			break
		}
	}

	if len(fields) != 3 || fields[0] != "blobs" {
		return d, status.Errorf(codes.InvalidArgument, "invalid resource name, must match format {instance_name}/blobs/{hash}/{size}: %s", name)
	}

	hash := fields[1]
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return d, status.Errorf(codes.InvalidArgument, "invalid resource name, fourth component (size) must be an integer: %s", fields[2])
	}
	if size < 0 {
		return d, status.Errorf(codes.InvalidArgument, "invalid resource name, fourth component (size) must be non-negative: %d", size)
	}
	d, err = digest.New(hash, size)
	if err != nil {
		return d, status.Errorf(codes.InvalidArgument, "invalid resource name, third component is not a valid digest: %s => %s", hash, err)
	}

	return d, nil
}

// parseWriteResource parses a WriteRequest.ResourceName and returns the validated Digest and upload ID.
// The resource name must be of the form: {instance_name}/uploads/{uuid}/blobs/{hash}/{size}[/{optionalmetadata}]
func parseWriteResource(name string) (d digest.Digest, u string, err error) {
	fields := strings.Split(name, "/")

	// Strip any parts before "uploads", as they'll belong to an instance name.
	for i := range fields {
		if fields[i] == "uploads" {
			fields = fields[i:]
			break
		}
	}

	if len(fields) < 5 || fields[0] != "uploads" || fields[2] != "blobs" {
		return d, u, status.Errorf(codes.InvalidArgument, "invalid resource name, must follow format {instance_name}/uploads/{uuid}/blobs/{hash}/{size}[/{optionalmetadata}]: %s", name)
	}

	uuid, err := uuid.Parse(fields[1])
	if err != nil {
		return d, u, status.Errorf(codes.InvalidArgument, "invalid resource name, second component is not a UUID: %s", fields[1])
	}
	u = uuid.String()

	hash := fields[3]

	size, err := strconv.ParseInt(fields[4], 10, 64)
	if err != nil {
		return d, u, status.Errorf(codes.InvalidArgument, "invalid resource name, fifth component (size) must be an integer: %s", fields[4])
	}
	if size < 0 {
		return d, u, status.Errorf(codes.InvalidArgument, "invalid resource name, fifth component (size) must be non-negative: %d", size)
	}
	d, err = digest.New(hash, size)
	if err != nil {
		return d, u, status.Errorf(codes.InvalidArgument, "invalid resource name, fourth component is not a valid digest: %s => %s", hash, err)
	}

	return d, u, nil
}

// Read implements the ByteStream.Read RPC.
func (s *Service) Read(request *bytestream.ReadRequest, server bytestream.ByteStream_ReadServer) error {
	err := s.read(request, server)
	if err != nil {
		log.Printf("🚨 Read(%v) => Error: %v", request.ResourceName, err)
	} else {
		log.Printf("✅ Read(%v) => OK", request.ResourceName)
	}
	return err
}

func (s *Service) read(request *bytestream.ReadRequest, server bytestream.ByteStream_ReadServer) error {
	d, err := parseReadResource(request.ResourceName)
	if err != nil {
		return err
	}

	// A `read_offset` that is negative or greater than the size of the resource
	// will cause an `OUT_OF_RANGE` error.
	if request.ReadOffset < 0 {
		return status.Error(codes.OutOfRange, "offset is negative")
	}
	if request.ReadOffset > d.Size {
		return status.Error(codes.OutOfRange, "offset is greater than the size of the file")
	}

	// Prepare a buffer to read the file into.
	bufSize := maxChunkSize
	if request.ReadLimit > 0 && request.ReadLimit < bufSize {
		bufSize = request.ReadLimit
	}
	buf := make([]byte, bufSize)

	// Open the file and seek to the offset.
	f, err := s.cas.Open(d, request.ReadOffset, request.ReadLimit)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to open file: %v", err)
	}
	defer f.Close() // OK to ignore error here, since we're only reading.

	// Send the requested data to the client in chunks.
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if err := server.Send(&bytestream.ReadResponse{
				Data: buf[:n],
			}); err != nil {
				return status.Errorf(codes.Internal, "failed to send data to client: %v", err)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read data from file: %v", err)
		}
	}

	return nil
}

// Write implements the ByteStream.Write RPC.
func (s *Service) Write(server bytestream.ByteStream_WriteServer) error {
	resourceName, err := s.write(server)
	if err != nil {
		log.Printf("🚨 Write(%v) => Error: %v", resourceName, err)
	} else {
		log.Printf("✅ Write(%v) => OK", resourceName)
	}
	return err
}

func (s *Service) write(server bytestream.ByteStream_WriteServer) (resource string, err error) {
	expectedDigest := digest.Empty
	ourHash := sha256.New()
	var committedSize int64
	finishedWriting := false
	var tempFile *os.File
	var tempPath string
	defer func() {
		if tempFile != nil {
			if err := tempFile.Close(); err != nil {
				log.Printf("could not close temporary file %q: %v", tempPath, err)
			}
			if err := os.Remove(tempPath); err != nil {
				log.Printf("could not delete temporary file %q: %v", tempPath, err)
			}
		}
	}()

	for {
		// Receive a request from the client.
		request, err := server.Recv()
		if err == io.EOF {
			// If the client closed the connection without ever sending a request, return an error.
			if resource == "" {
				return resource, status.Error(codes.InvalidArgument, "no resource name provided")
			}

			// Check that the client set "finish_write" to true.
			if !finishedWriting {
				return resource, status.Error(codes.InvalidArgument, "upload finished without finish_write set")
			}

			// Check that the digests (= hash and size) match.
			d := digest.Digest{Hash: hex.EncodeToString(ourHash.Sum(nil)), Size: committedSize}
			if d != expectedDigest {
				return resource, status.Errorf(codes.InvalidArgument, "computed digest %v did not match expected digest %v", d, expectedDigest)
			}

			// Move the temporary file to the CAS.
			if err := s.cas.Adopt(expectedDigest, tempPath); err != nil {
				return resource, status.Errorf(codes.Internal, "failed to move file into CAS: %v", err)
			}

			// Send the response to the client.
			if err := server.SendAndClose(&bytestream.WriteResponse{
				CommittedSize: committedSize,
			}); err != nil {
				return resource, status.Errorf(codes.Internal, "failed to send response to client: %v", err)
			}

			// Yay, we're done!
			return resource, nil
		} else if err != nil {
			return resource, status.Errorf(codes.Internal, "failed to receive request from client: %v", err)
		}

		// If the resource name is empty, this is the first request from the client.
		if resource == "" {
			if request.ResourceName == "" {
				return resource, status.Errorf(codes.InvalidArgument, "must set resource name on first request")
			}
			resource = request.ResourceName
			var uuid string
			expectedDigest, uuid, err = parseWriteResource(request.ResourceName)
			if err != nil {
				return resource, err
			}
			tempPath = filepath.Join(s.uploadDir, uuid)
		} else {
			// Ensure that the resource name is either not set, or the same as the first request.
			if request.ResourceName != "" && request.ResourceName != resource {
				return resource, status.Errorf(codes.InvalidArgument, "resource name changed (%v => %v)", resource, request.ResourceName)
			}
		}

		if finishedWriting {
			return resource, status.Error(codes.InvalidArgument, "cannot write more data after finish_write was true")
		}

		// If the resource was uploaded concurrently and already exists in our CAS, immediately return success.
		if _, err := s.cas.Stat(expectedDigest); err == nil {
			return resource, server.SendAndClose(&bytestream.WriteResponse{
				CommittedSize: expectedDigest.Size,
			})
		}

		// Create the file for the pending upload if this is the first write.
		if tempFile == nil && !finishedWriting {
			tempFile, err = os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
			if err != nil {
				if os.IsExist(err) {
					return resource, status.Errorf(codes.InvalidArgument, "upload with same uuid already in progress")
				}
				return resource, status.Errorf(codes.Internal, "could not create temporary file for upload: %v", err)
			}
		}

		// Append the received data to the temporary file and hash it.
		if _, err := tempFile.Write(request.Data); err != nil {
			return resource, status.Errorf(codes.Internal, "failed to write data to temporary file: %v", err)
		}
		ourHash.Write(request.Data)
		committedSize += int64(len(request.Data))

		// If the file is already larger than the expected size, something is wrong - return an error.
		if committedSize > expectedDigest.Size {
			return resource, status.Errorf(codes.InvalidArgument, "received %d bytes, more than expected %d", committedSize, expectedDigest.Size)
		}

		if request.FinishWrite {
			finishedWriting = true
			err = tempFile.Close()
			if err != nil {
				return resource, status.Errorf(codes.Internal, "could not close temporary file: %v", err)
			}
			// We set tempFile to `nil` *after* checking for an error to give the defer handler a last
			// chance to clean things up...
			tempFile = nil
		}
	}
}

// QueryWriteStatus implements the ByteStream.QueryWriteStatus RPC.
func (s *Service) QueryWriteStatus(ctx context.Context, request *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	response, err := s.queryWriteStatus(request)
	if err != nil {
		log.Printf("🚨 QueryWriteStatus(%v) failed: %s", request.ResourceName, err)
	} else {
		log.Printf("✅ QueryWriteStatus(%v) succeeded", request.ResourceName)
	}
	return response, err
}

func (s *Service) queryWriteStatus(request *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	d, _, err := parseWriteResource(request.ResourceName)
	if err != nil {
		return nil, err
	}

	// Check if the file exists in the CAS, if yes, the upload is complete.
	if _, err := s.cas.Stat(d); err == nil {
		return &bytestream.QueryWriteStatusResponse{
			CommittedSize: d.Size,
			Complete:      true,
		}, nil
	}

	// We don't support resuming uploads yet, so just always return that we don't have any data.
	return &bytestream.QueryWriteStatusResponse{
		CommittedSize: 0,
		Complete:      false,
	}, nil
}

// FindMissingBlobs implements the ContentAddressableStorage.FindMissingBlobs RPC.
func (s *Service) FindMissingBlobs(ctx context.Context, request *remote.FindMissingBlobsRequest) (*remote.FindMissingBlobsResponse, error) {
	response, err := s.findMissingBlobs(request)
	if err != nil {
		log.Printf("🚨 FindMissingBlobs(%d blobs) => Error: %v", len(request.BlobDigests), err)
	} else {
		log.Printf("✅ FindMissingBlobs(%d blobs) => OK (%d missing)", len(request.BlobDigests), len(response.MissingBlobDigests))
	}
	return response, err
}

func (s *Service) findMissingBlobs(request *remote.FindMissingBlobsRequest) (*remote.FindMissingBlobsResponse, error) {
	// If the client explicitly specifies a DigestFunction, ensure that it's SHA256.
	if request.DigestFunction != remote.DigestFunction_UNKNOWN && request.DigestFunction != remote.DigestFunction_SHA256 {
		return nil, status.Errorf(codes.InvalidArgument, "hash function %q is not supported", request.DigestFunction.String())
	}

	// Make a list that stores the missing blobs. We set the capacity so that we never have to reallocate.
	missing := make([]*remote.Digest, 0, len(request.BlobDigests))

	// For each blob in the list, check if it exists in the CAS. If not, add it to the list of missing blobs.
	for _, d := range request.BlobDigests {
		dg, err := digest.NewFromProto(d)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid digest: %v", err)
		}
		if _, err := s.cas.Stat(dg); err != nil {
			missing = append(missing, d)
		}
	}

	// Return the list of missing blobs to the client.
	return &remote.FindMissingBlobsResponse{
		MissingBlobDigests: missing,
	}, nil
}

// BatchUpdateBlobs implements the ContentAddressableStorage.BatchUpdateBlobs RPC.
func (s *Service) BatchUpdateBlobs(ctx context.Context, request *remote.BatchUpdateBlobsRequest) (*remote.BatchUpdateBlobsResponse, error) {
	response, err := s.batchUploadBlobs(request)
	if err != nil {
		log.Printf("🚨 BatchUpdateBlobs(%v blobs) => Error: %v", len(request.Requests), err)
	} else {
		log.Printf("✅ BatchUpdateBlobs(%v blobs) => OK", len(request.Requests))
	}
	return response, err
}

func (s *Service) batchUploadBlobs(request *remote.BatchUpdateBlobsRequest) (*remote.BatchUpdateBlobsResponse, error) {
	// If the client explicitly specifies a DigestFunction, ensure that it's SHA256.
	if request.DigestFunction != remote.DigestFunction_UNKNOWN && request.DigestFunction != remote.DigestFunction_SHA256 {
		return nil, status.Errorf(codes.InvalidArgument, "hash function %q is not supported", request.DigestFunction.String())
	}

	// Prepare a response that we can fill in.
	response := &remote.BatchUpdateBlobsResponse{
		Responses: make([]*remote.BatchUpdateBlobsResponse_Response, 0, len(request.Requests)),
	}

	// For each blob in the list, check if it exists in the CAS. If not, write it to the CAS.
	for _, blob := range request.Requests {
		// Ensure that the client didn't send compressed data.
		if blob.Compressor != remote.Compressor_IDENTITY {
			return nil, status.Errorf(codes.InvalidArgument, "compressed data is not supported")
		}

		// Parse the digest.
		expectedDigest, err := digest.NewFromProto(blob.Digest)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid digest: %v", err)
		}

		// Store the blob in our CAS.
		actualDigest, err := s.cas.Put(blob.Data)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "could not store blob in CAS: %v", err)
		}

		// Check that the calculated digest matches the data.
		if actualDigest != expectedDigest {
			return nil, status.Errorf(codes.InvalidArgument, "digest does not match data")
		}

		// Add the response to the list.
		response.Responses = append(response.Responses, &remote.BatchUpdateBlobsResponse_Response{
			Digest: blob.Digest,
			Status: status.New(codes.OK, "").Proto(),
		})
	}

	// Return the response to the client.
	return response, nil
}

func (s *Service) BatchReadBlobs(ctx context.Context, request *remote.BatchReadBlobsRequest) (*remote.BatchReadBlobsResponse, error) {
	response, err := s.batchReadBlobs(request)
	if err != nil {
		log.Printf("🚨 BatchReadBlobs(%v blobs) => Error: %v", len(request.Digests), err)
	} else {
		log.Printf("✅ BatchReadBlobs(%v blobs) => OK", len(request.Digests))
	}
	return response, err
}

func (s *Service) batchReadBlobs(request *remote.BatchReadBlobsRequest) (*remote.BatchReadBlobsResponse, error) {
	// If the client explicitly specifies a DigestFunction, ensure that it's SHA256.
	if request.DigestFunction != remote.DigestFunction_UNKNOWN && request.DigestFunction != remote.DigestFunction_SHA256 {
		return nil, status.Errorf(codes.InvalidArgument, "hash function %q is not supported", request.DigestFunction.String())
	}

	// Prepare a response that we can fill in.
	response := &remote.BatchReadBlobsResponse{
		Responses: make([]*remote.BatchReadBlobsResponse_Response, 0, len(request.Digests)),
	}

	// For each blob in the list, check if it exists in the CAS. If yes, read it from the CAS.
	for _, d := range request.Digests {
		// Parse the digest.
		dg, err := digest.NewFromProto(d)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid digest: %v", err)
		}

		// Read the blob from the CAS.
		data, err := s.cas.Get(dg)
		if err != nil {
			if os.IsNotExist(err) {
				// The blob doesn't exist. Add a response with an appropriate status code.
				response.Responses = append(response.Responses, &remote.BatchReadBlobsResponse_Response{
					Digest: d,
					Status: status.New(codes.NotFound, "").Proto(),
				})
				continue
			} else {
				return nil, status.Errorf(codes.Internal, "failed to read blob: %v", err)
			}
		} else {
			// The blob exists. Add a response with the data.
			response.Responses = append(response.Responses, &remote.BatchReadBlobsResponse_Response{
				Digest: d,
				Data:   data,
				Status: status.New(codes.OK, "").Proto(),
			})
		}
	}

	// Return the response to the client.
	return response, nil
}

func (s *Service) GetTree(request *remote.GetTreeRequest, treeServer remote.ContentAddressableStorage_GetTreeServer) error {
	if err := s.getTree(request, treeServer); err != nil {
		log.Printf("🚨 GetTree(%v) => Error: %v", request.RootDigest, err)
		return err
	} else {
		log.Printf("✅ GetTree(%v) => OK", request.RootDigest)
	}
	return nil
}

func (s *Service) getTree(request *remote.GetTreeRequest, treeServer remote.ContentAddressableStorage_GetTreeServer) error {
	// If the client explicitly specifies a DigestFunction, ensure that it's SHA256.
	if request.DigestFunction != remote.DigestFunction_UNKNOWN && request.DigestFunction != remote.DigestFunction_SHA256 {
		return status.Errorf(codes.InvalidArgument, "hash function %q is not supported", request.DigestFunction.String())
	}

	// Prepare a response that we can fill in.
	response := &remote.GetTreeResponse{
		Directories: make([]*remote.Directory, 0),
	}

	// Create a queue of directories to process and add the root directory.
	dirQueue := []*remote.DirectoryNode{
		{
			Digest: request.RootDigest,
		},
	}

	// Iteratively process the directories.
	for len(dirQueue) > 0 {
		// Take a directoryNode from the queue.
		directoryNode := dirQueue[0]
		dirQueue = dirQueue[1:]

		// Parse the digest.
		d, err := digest.NewFromProto(directoryNode.Digest)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid digest: %v", err)
		}

		// Get the blob for the directory message from the CAS.
		directoryBlob, err := s.cas.Get(d)
		if err != nil {
			return status.Errorf(codes.NotFound, "directory not found: %v", err)
		}

		// Unmarshal the directory message.
		var directory *remote.Directory
		if err := proto.Unmarshal(directoryBlob, directory); err != nil {
			return status.Errorf(codes.Internal, "failed to unmarshal directory: %v", err)
		}

		// Add the directory to the response.
		response.Directories = append(response.Directories, directory)

		// Add all subdirectory nodes to the queue.
		dirQueue = append(dirQueue, directory.Directories...)
	}

	// TODO: Add support for pagination?

	// Send the tree to the client.
	return treeServer.Send(response)
}
