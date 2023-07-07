// Copyright 2023 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package blobstore

import (
	"io"
)

// LimitedReadCloser is an io.ReadCloser that limits the number of bytes that can be read.
type LimitedReadCloser struct {
	*io.LimitedReader
	io.Closer
}

// LimitReadCloser wraps an io.LimitedReader in a LimitedReadCloser.
func LimitReadCloser(r io.ReadCloser, limit int64) io.ReadCloser {
	return &LimitedReadCloser{
		LimitedReader: &io.LimitedReader{R: r, N: limit},
		Closer:        r,
	}
}
