// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"fmt"
	"io"
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
	NewKeyWriter: func() KeyWriter {
		kw := &cockroachKeyWriter{}
		kw.prefixes.Init(16)
		kw.wallTimes.Init(UintDefaultNone)
		kw.logicalTimes.Init(UintDefaultZero)
		kw.untypedSuffixesValues.Init(0)
		kw.untypedSuffixes = MakeDefaultNull(DataTypeBytes, &kw.untypedSuffixesValues)
		return kw
	},
}

type cockroachKeyWriter struct {
	prefixes              bytesBuilder
	wallTimes             Uint64Builder
	logicalTimes          Uint32Builder
	untypedSuffixes       DefaultNull[*bytesBuilder]
	untypedSuffixesValues bytesBuilder
}

func (w *cockroachKeyWriter) WriteKey(row int, key []byte) (samePrefix bool) {
	prefix, untypedSuffix, wallTime, logicalTime := crdbtest.DecodeTimestamp(key)
	samePrefix = w.prefixes.PutOrdered(prefix)
	w.wallTimes.Set(row, wallTime)
	// The w.logicalTimes builder was initialized with UintDefaultZero, so if we
	// don't set a value, the column value is implicitly zero. We only need to
	// Set anything for non-zero values.
	if logicalTime > 0 {
		w.logicalTimes.Set(row, logicalTime)
	}
	if untypedSuffix != nil {
		bw, _ := w.untypedSuffixes.NotNull(row)
		bw.Put(untypedSuffix)
	}
	return samePrefix
}

func (w *cockroachKeyWriter) Reset() {
	w.prefixes.Reset()
	w.untypedSuffixes.Reset()
	w.wallTimes.Reset()
	w.logicalTimes.Reset()
}

func (kw *cockroachKeyWriter) WriteDebug(dst io.Writer, rows int) {
	fmt.Fprint(dst, "prefixes: ")
	kw.prefixes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "wall times: ")
	kw.wallTimes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "logical times: ")
	kw.logicalTimes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
	fmt.Fprint(dst, "untyped suffixes: ")
	kw.untypedSuffixes.WriteDebug(dst, rows)
	fmt.Fprintln(dst)
}

func (kw *cockroachKeyWriter) NumColumns() int {
	return cockroachColCount
}

func (kw *cockroachKeyWriter) Size(rows int, offset uint32) uint32 {
	offset = kw.prefixes.Size(rows, offset)
	offset = kw.wallTimes.Size(rows, offset)
	offset = kw.logicalTimes.Size(rows, offset)
	offset = kw.untypedSuffixes.Size(rows, offset)
	return offset
}

func (kw *cockroachKeyWriter) Finish(
	col int, rows int, offset uint32, buf []byte,
) (uint32, ColumnDesc) {
	switch col {
	case cockroachColPrefix:
		return kw.prefixes.Finish(rows, offset, buf)
	case cockroachColMVCCWallTime:
		return kw.wallTimes.Finish(rows, offset, buf)
	case cockroachColMVCCLogical:
		return kw.logicalTimes.Finish(rows, offset, buf)
	case cockroachColUntypedSuffix:
		return kw.untypedSuffixes.Finish(rows, offset, buf)
	default:
		panic(fmt.Sprintf("unknown default key column: %d", col))
	}
}

func (kw *cockroachKeyWriter) Release() {}

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
