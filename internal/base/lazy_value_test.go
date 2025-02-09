// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type valueRetrieverFuncs struct {
	RetrieveFunc         func(handle []byte) (val []byte, err error)
	DecodeAttributesFunc func(handle []byte) ValueAttributes
}

func (v valueRetrieverFuncs) DecodeAttributes(handle []byte) ValueAttributes {
	return v.DecodeAttributesFunc(handle)
}

func (v valueRetrieverFuncs) Retrieve(ctx context.Context, file DiskFileNum, handle []byte) (val []byte, err error) {
	return v.RetrieveFunc(handle)
}

func TestLazyValue(t *testing.T) {
	ctx := context.Background()
	// Both 40 and 48 bytes makes iteration benchmarks like
	// BenchmarkIteratorScan/keys=1000,r-amp=1,key-types=points-only 75%
	// slower.
	// require.True(t, unsafe.Sizeof(LazyValue{}) <= 32)

	fooBytes1 := []byte("foo")
	fooLV1 := MakeInPlaceValue(fooBytes1)
	require.Equal(t, 3, fooLV1.Len())
	_, hasAttr := fooLV1.TryGetShortAttribute()
	require.False(t, hasAttr)
	fooLV2, fooBytes2 := fooLV1.Clone(nil)
	require.Equal(t, 3, fooLV2.Len())
	_, hasAttr = fooLV2.TryGetShortAttribute()
	require.False(t, hasAttr)
	require.Equal(t, fooLV1.InPlaceValue(), fooLV2.InPlaceValue())
	getValue := func(lv *LazyValue) []byte {
		v, err := lv.Get(ctx)
		require.NoError(t, err)
		return v
	}
	require.Equal(t, getValue(&fooLV1), getValue(&fooLV2))
	fooBytes2[0] = 'b'
	require.False(t, bytes.Equal(fooLV1.InPlaceValue(), fooLV2.InPlaceValue()))

	numCalls := 0
	fooLV3 := LazyValue{
		ValueOrHandle: []byte("foo-handle"),
		Retriever: valueRetrieverFuncs{
			RetrieveFunc: func(handle []byte) ([]byte, error) {
				numCalls++
				require.Equal(t, []byte("foo-handle"), handle)
				return fooBytes1, nil
			},
			DecodeAttributesFunc: func(handle []byte) ValueAttributes {
				return ValueAttributes{ValueLen: 3, ShortAttribute: 7, ShortAttributeExists: true}
			},
		},
	}
	require.Equal(t, []byte("foo"), getValue(&fooLV3))
	require.Equal(t, 1, numCalls)
	require.Equal(t, []byte("foo"), getValue(&fooLV3))
	require.Equal(t, 1, numCalls)
	require.Equal(t, 3, fooLV3.Len())
	// attr, hasAttr := fooLV3.TryGetShortAttribute()
	// require.True(t, hasAttr)
	// require.Equal(t, ShortAttribute(7), attr)
}
