// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io"
	"os"
	"path/filepath"
)

// RelativeFS wraps the provided FS with a FS that translates all relative paths
// to paths rooted at the provided root directory. The provided root directory
// may be absolute or relative itself.
func RelativeFS(fs FS, root string) FS {
	return relativeFS{inner: fs, root: root}
}

type relativeFS struct {
	inner FS
	root  string
}

// Assert that relativeFS implements FS.
var _ FS = relativeFS{}

func (fs relativeFS) maybePrefix(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return fs.inner.PathJoin(fs.root, name)
}

func (fs relativeFS) Create(name string) (File, error) {
	return fs.inner.Create(fs.maybePrefix(name))
}

func (fs relativeFS) Link(oldname, newname string) error {
	return fs.inner.Link(fs.maybePrefix(oldname), fs.maybePrefix(newname))
}

func (fs relativeFS) Open(name string, opts ...OpenOption) (File, error) {
	return fs.inner.Open(fs.maybePrefix(name), opts...)
}

func (fs relativeFS) OpenDir(name string) (File, error) {
	return fs.inner.OpenDir(fs.maybePrefix(name))
}

func (fs relativeFS) Remove(name string) error {
	return fs.inner.Remove(fs.maybePrefix(name))
}

func (fs relativeFS) RemoveAll(name string) error {
	return fs.inner.RemoveAll(fs.maybePrefix(name))
}

func (fs relativeFS) Rename(oldname, newname string) error {
	return fs.inner.Rename(fs.maybePrefix(oldname), fs.maybePrefix(newname))
}

func (fs relativeFS) ReuseForWrite(oldname, newname string) (File, error) {
	return fs.inner.ReuseForWrite(fs.maybePrefix(oldname), fs.maybePrefix(newname))
}

func (fs relativeFS) MkdirAll(dir string, perm os.FileMode) error {
	return fs.inner.MkdirAll(fs.maybePrefix(dir), perm)
}

func (fs relativeFS) Lock(name string) (io.Closer, error) {
	return fs.inner.Lock(fs.maybePrefix(name))
}

func (fs relativeFS) List(dir string) ([]string, error) {
	return fs.inner.List(fs.maybePrefix(dir))
}

func (fs relativeFS) Stat(name string) (os.FileInfo, error) {
	return fs.inner.Stat(fs.maybePrefix(name))
}

func (fs relativeFS) PathBase(path string) string {
	return fs.inner.PathBase(path)
}

func (fs relativeFS) PathJoin(elem ...string) string {
	return fs.inner.PathJoin(elem...)
}

func (fs relativeFS) PathDir(path string) string {
	return fs.inner.PathDir(path)
}

func (fs relativeFS) GetDiskUsage(path string) (DiskUsage, error) {
	return fs.inner.GetDiskUsage(fs.maybePrefix(path))
}

// Unwrap returns the inner FS, allowing users to retrieve the root FS with
// vfs.Root.
func (fs relativeFS) Unwrap() FS {
	return fs.inner
}
