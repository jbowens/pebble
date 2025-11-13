// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPosixRing(t *testing.T) {
	fs := NewMem()
	ring := newPosixIORing(fs)
	defer func() { require.NoError(t, ring.Close()) }()

	fileID, err := ring.Submit(RingOp{
		Type:          RingOpCreate,
		Path:          "test.txt",
		WriteCategory: WriteCategoryUnspecified,
	})
	require.NoError(t, err)

	_, err = ring.Submit(RingOp{
		Type:     RingOpAppend,
		FileOpID: fileID,
		Data:     []byte("hello"),
	})
	require.NoError(t, err)

	_, err = ring.Submit(RingOp{
		Type:     RingOpClose,
		FileOpID: fileID,
	})
	require.NoError(t, err)
	require.NoError(t, ring.Error())
}
