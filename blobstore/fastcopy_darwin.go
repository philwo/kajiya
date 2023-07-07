// Copyright 2023 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

//go:build darwin

package blobstore

import (
	"golang.org/x/sys/unix"
)

// fastCopy copies a file from source to destination using a clonefile syscall.
// This is nicer than using a hard link, because it means that even if the file
// is accidentally modified, the copy will still have the original contents.
func fastCopy(source, destination string) error {
	return unix.Clonefile(source, destination, unix.CLONE_NOFOLLOW)
}
