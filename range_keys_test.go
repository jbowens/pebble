// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestRangeKeys_Simple(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	require.NoError(t, err)
	defer d.Close()

	b := d.NewBatch()
	b.Set([]byte("b"), []byte("foo"), nil)
	b.ExperimentalRangeKey([]byte("a"), []byte("c"), []byte("bar"), nil)

	require.NoError(t, d.Apply(b, nil))

	iter := d.NewIter(nil)
	require.True(t, iter.SeekGE([]byte("b")))
	require.Equal(t, []byte("foo"), iter.Value())
	defer iter.Close()
}
