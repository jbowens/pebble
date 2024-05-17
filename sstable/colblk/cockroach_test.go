// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crdbtest"
	"golang.org/x/exp/rand"
)

const (
	cockroachColPrefix int = iota
	cockroachColMVCCWallTime
	cockroachColMVCCLogical
	cockroachColUntypedSuffix
	cockroachColCount
)

var cockroachKeySchema = KeySchema{
	Columns: []ColumnConfig{
		cockroachColPrefix:        {DataType: DataTypePrefixBytes, BundleSize: 16},
		cockroachColMVCCWallTime:  {DataType: DataTypeUint64},
		cockroachColMVCCLogical:   {DataType: DataTypeUint32},
		cockroachColUntypedSuffix: {DataType: DataTypeBytes},
	},
	WriteKey: func(key []byte, w ColumnWriter) bool {
		userKey, untypedSuffix, wallTime, logical := crdbtest.DecodeTimestamp(key)
		samePrefix := w.PutPrefixBytes(cockroachColPrefix, userKey)
		if untypedSuffix != nil {
			w.PutNull(cockroachColMVCCWallTime)
			w.PutNull(cockroachColMVCCLogical)
			w.PutRawBytes(cockroachColUntypedSuffix, untypedSuffix)
		} else {
			w.PutUint64(cockroachColMVCCWallTime, wallTime)
			w.PutUint32(cockroachColMVCCLogical, logical)
			w.PutNull(cockroachColUntypedSuffix)
		}
		return samePrefix
	},
}

func BenchmarkCockroachDataBlockWriter(b *testing.B) {
	for _, prefixSize := range []int{8, 32, 128} {
		for _, valueSize := range []int{8, 128, 1024} {
			b.Run(fmt.Sprintf("prefix=%d,value=%d", prefixSize, valueSize), func(b *testing.B) {
				benchmarkCockroachDataBlockWriter(b, prefixSize, valueSize)
			})
		}
	}
}

func benchmarkCockroachDataBlockWriter(b *testing.B, prefixSize, valueSize int) {
	const targetBlockSize = 32 << 10
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewSource(seed))
	keys, values := makeCockroachRandomKVs(rng, prefixSize, valueSize, targetBlockSize)

	var w DataBlockWriter
	w.Init(cockroachKeySchema)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Reset()
		var j int
		for w.Size() < targetBlockSize {
			ik := base.MakeInternalKey(keys[j], rng.Uint64n(base.InternalKeySeqNumMax), base.InternalKeyKindSet)
			w.Add(ik, values[j])
			j++
		}
		w.Finish()
	}
}

func makeCockroachRandomKVs(
	rng *rand.Rand, prefixSize, valueSize int, aggregateSize int,
) (keys, vals [][]byte) {
	keys = make([][]byte, aggregateSize/valueSize+1)
	vals = make([][]byte, len(keys))
	for i := range keys {
		keys[i] = randCockroachKey(rng, make([]byte, prefixSize+crdbtest.MaxSuffixLen), prefixSize)
		vals[i] = make([]byte, valueSize)
		rng.Read(vals[i])
	}
	slices.SortFunc(keys, crdbtest.Compare)
	return keys, vals
}

func randCockroachKey(rng *rand.Rand, buf []byte, prefixLen int) []byte {
	wallTime := uint64(time.Now().UnixNano()) + rng.Uint64n(uint64(time.Hour))
	key := buf[:prefixLen]
	for i := 0; i < prefixLen; i++ {
		buf[i] = byte(rng.Intn(26) + 'a')
	}
	return crdbtest.EncodeTimestamp(key, wallTime, 0)
}
