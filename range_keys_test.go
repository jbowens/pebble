// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type concatRangeValues struct{}

func (concatRangeValues) Name() string { return "pebble_concat_range_values" }
func (concatRangeValues) MergeValues(dst []byte, includesBase bool, operands ...[]byte) []byte {
	for _, operand := range operands {
		dst = append(dst, operand...)
	}
	return dst
}

func TestRangeKeys_Simple(t *testing.T) {
	opts := &Options{FS: vfs.NewMem()}
	opts.Experimental.RangeValueMerger = concatRangeValues{}
	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	b := d.NewBatch()
	b.Set([]byte("b"), []byte("foo"), nil)
	b.ExperimentalRangeKey([]byte("a"), []byte("c"), []byte("bar"), nil)
	require.NoError(t, d.Apply(b, nil))

	b = d.NewBatch()
	b.ExperimentalRangeKey([]byte("apple"), []byte("banana"), []byte("coconut"), nil)
	require.NoError(t, d.Apply(b, nil))

	iter := d.NewIter(nil)
	require.True(t, iter.SeekGE([]byte("b")))
	require.Equal(t, []byte("foo"), iter.Value())

	rv := iter.ExperimentalRangeValue()
	require.False(t, rv.Empty())

	for _, span := range rv.spans {
		t.Logf("[%s, %s): %s\n", span.Start, span.End, span.Value)
	}

	require.Equal(t, []RangeKeySpan{
		{Start: []byte("apple"), End: []byte("banana"), Value: []byte("coconutbar")},
	}, rv.spans)
	defer iter.Close()
}
