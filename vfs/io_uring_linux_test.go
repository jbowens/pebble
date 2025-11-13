// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build linux

package vfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIOUring(t *testing.T) {
	ring, err := newIOUring(128)
	require.NoError(t, err)
	defer func() { require.NoError(t, ring.Close()) }()
}
