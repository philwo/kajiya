// Copyright 2023 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package actioncache

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	remote "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
)

// ActionCache is a simple action cache implementation that stores ActionResults on the local disk.
type ActionCache struct {
	dataDir string
}

// New creates a new local ActionCache. The data directory is created if it does not exist.
func New(dataDir string) (*ActionCache, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("data directory must be specified")
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	// Create subdirectories {00, 01, ..., ff} for sharding by hash prefix.
	for i := 0; i <= 255; i++ {
		err := os.Mkdir(filepath.Join(dataDir, fmt.Sprintf("%02x", i)), 0755)
		if err != nil {
			if os.IsExist(err) {
				continue
			}
			return nil, err
		}
	}

	return &ActionCache{
		dataDir: dataDir,
	}, nil
}

// path returns the path to the file with digest d in the action cache.
func (c *ActionCache) path(d digest.Digest) string {
	return filepath.Join(c.dataDir, d.Hash[:2], d.Hash)
}

// Get returns the cached ActionResult for the given digest.
func (c *ActionCache) Get(actionDigest digest.Digest) (*remote.ActionResult, error) {
	p := c.path(actionDigest)

	// Read the action result for the requested action into a byte slice.
	buf, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}

	// Unmarshal it into an ActionResult message and return it to the client.
	actionResult := &remote.ActionResult{}
	if err := proto.Unmarshal(buf, actionResult); err != nil {
		return nil, err
	}

	return actionResult, nil
}

// Put stores the given ActionResult for the given digest.
func (c *ActionCache) Put(actionDigest digest.Digest, ar *remote.ActionResult) error {
	// Marshal the action result.
	actionResultRaw, err := proto.Marshal(ar)
	if err != nil {
		return err
	}

	// Store the action result in our action cache.
	f, err := os.CreateTemp(c.dataDir, "tmp_")
	if err != nil {
		return err
	}
	if _, err := f.Write(actionResultRaw); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), c.path(actionDigest)); err != nil {
		// TODO: It's possible that on Windows we cannot rename the file to the destination because it already exists.
		// In that case, we should check if the file is identical to the one we're trying to write, and if so, ignore the error.
		return err
	}

	return nil
}

// Remove deletes the cached ActionResult for the given digest.
func (c *ActionCache) Remove(d digest.Digest) error {
	return fmt.Errorf("not implemented yet")
}
