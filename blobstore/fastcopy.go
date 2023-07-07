// Copyright 2023 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build !darwin

package blobstore

import "os"

// fastCopy copies a file from source to destination using a hard link.
// This is usually the best we can do, unless the operating system supports
// copy-on-write semantics for files (e.g. macOS with APFS).
func fastCopy(source, destination string) error {
	return os.Link(source, destination)
}
