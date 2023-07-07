// Copyright 2023 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package blobstore

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
)

// ContentAddressableStorage is a simple CAS implementation that stores files on the local disk.
type ContentAddressableStorage struct {
	dataDir string
}

// New creates a new local CAS. The data directory is created if it does not exist.
func New(dataDir string) (*ContentAddressableStorage, error) {
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

	cas := &ContentAddressableStorage{
		dataDir: dataDir,
	}

	// Ensure that we have the "empty blob" present in the CAS.
	// Clients will usually not upload it, but just assume that it's always available.
	// A faster way would be to special case the empty digest in the CAS implementation,
	// but this is simpler and more robust.
	d, err := cas.Put(nil)
	if err != nil {
		return nil, err
	}
	if d != digest.Empty {
		return nil, fmt.Errorf("empty blob did not have expected hash: got %s, wanted %s", d, digest.Empty)
	}

	return cas, nil
}

// path returns the path to the file with digest d in the CAS.
func (c *ContentAddressableStorage) path(d digest.Digest) string {
	return filepath.Join(c.dataDir, d.Hash[:2], d.Hash)
}

// Stat returns os.FileInfo for the requested digest if it exists.
func (c *ContentAddressableStorage) Stat(d digest.Digest) (os.FileInfo, error) {
	p := c.path(d)

	fi, err := os.Lstat(p)
	if err != nil {
		return nil, err
	}

	if fi.Size() != d.Size {
		log.Printf("actual file size %d does not match requested size of digest %s", fi.Size(), d.String())
		return nil, fs.ErrNotExist
	}

	return fi, nil
}

// Open returns an io.ReadCloser for the requested digest if it exists.
// The returned ReadCloser is limited to the given offset and limit.
// The offset must be non-negative and no larger than the file size.
// A limit of 0 means no limit, and a limit that's larger than the file size is truncated to the file size.
func (c *ContentAddressableStorage) Open(d digest.Digest, offset int64, limit int64) (io.ReadCloser, error) {
	p := c.path(d)

	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}

	// Ensure that the file has the expected size.
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}

	if size != d.Size {
		log.Printf("actual file size %d does not match requested size of digest %s", offset, d.String())
		f.Close()
		return nil, fs.ErrNotExist
	}

	// Ensure that the offset is not negative and not larger than the file size.
	if offset < 0 || offset > size {
		f.Close()
		return nil, fs.ErrInvalid
	}

	// Seek to the requested offset.
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}

	// Cap the limit to the file size, taking the offset into account.
	if limit == 0 || limit > size-offset {
		limit = size - offset
	}

	return LimitReadCloser(f, limit), nil
}

// Get reads a file for the given digest from disk and returns its contents.
func (c *ContentAddressableStorage) Get(d digest.Digest) ([]byte, error) {
	// Just call Open and read the whole file.
	f, err := c.Open(d, 0, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close() // error is safe to ignore, because we're just reading
	return io.ReadAll(f)
}

// Put stores the given data in the CAS and returns its digest.
func (c *ContentAddressableStorage) Put(data []byte) (digest.Digest, error) {
	d := digest.NewFromBlob(data)
	p := c.path(d)

	// Check if the file already exists.
	// This is a fast path that avoids writing the file if it already exists.
	if _, err := os.Stat(p); err == nil {
		return d, nil
	}

	// Write the file to a temporary location and then rename it.
	// This ensures that we don't accidentally serve a partial file if the process is killed while writing.
	// It also ensures that we don't serve a file that's still being written.
	f, err := os.CreateTemp(c.dataDir, "tmp_")
	if err != nil {
		return d, err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return d, err
	}
	if err := f.Close(); err != nil {
		return d, err
	}
	if err := os.Rename(f.Name(), p); err != nil {
		// This might happen on Windows if the file already exists and we can't replace it.
		// Because this is a CAS, we can assume that the file contains the same data that we wanted to write.
		// So we can ignore this error.
		if os.IsExist(err) {
			return d, nil
		}
		return d, err
	}

	return d, nil
}

// Adopt moves a file from the given path into the CAS.
// The digest is assumed to have been validated by the caller.
func (c *ContentAddressableStorage) Adopt(d digest.Digest, path string) error {
	err := os.Rename(path, c.path(d))
	if err != nil {
		if os.IsExist(err) {
			// The file already exists, so we can ignore this error.
			// This might happen on Windows if the file already exists,
			// and we can't replace it.
			return nil
		}
		return err
	}
	return nil
}

// LinkTo creates a link `path` pointing to the file with digest `d` in the CAS.
// If the operating system supports cloning files via copy-on-write semantics,
// the file is cloned instead of hard linked.
func (c *ContentAddressableStorage) LinkTo(d digest.Digest, path string) error {
	return fastCopy(c.path(d), path)
}
