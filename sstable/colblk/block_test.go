// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"golang.org/x/exp/rand"
)

func TestBlockWriter(t *testing.T) {
	var buf bytes.Buffer
	var w blockWriter
	var rows uint32
	var colConfigs []ColumnConfig
	datadriven.RunTest(t, "testdata/block_writer", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "init":
			colTypeNames, ok := td.Arg("schema")
			if !ok {
				return "schema argument missing"
			}
			colConfigs = make([]ColumnConfig, len(colTypeNames.Vals))
			for i, v := range colTypeNames.Vals {
				colConfigs[i].DataType = dataTypeFromName(v)
				if colConfigs[i].DataType == DataTypePrefixBytes {
					colConfigs[i].BundleSize = 16
				}
			}
			rows = 0
			w.init(0, 0, colConfigs)
			fmt.Fprint(&buf, &w)
			return buf.String()
		case "write":
			for _, line := range strings.Split(td.Input, "\n") {
				for i, v := range strings.Fields(line) {
					writeColumnValue(colConfigs[i].DataType, i, v, &w)
				}
				rows++
			}
			fmt.Fprint(&buf, &w)
			return buf.String()
		case "finish":
			block := w.Finish(rows)
			r := NewBlockReader(block, 0)
			f := binfmt.New(r.data()).LineWidth(20)
			r.headerToBinFormatter(f)
			for i := 0; i < int(r.header.Columns); i++ {
				r.columnToBinFormatter(f, i, int(r.header.Rows))
			}
			return f.String()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// randBlock generates a random block of n rows with the provided schema. It
// returns the serialized raw block and a []interface{} slice containing the
// generated data. The type of each element of the slice is dependent on the
// corresponding column's type.
func randBlock(rng *rand.Rand, rows int, schema []ColumnConfig) ([]byte, []interface{}) {
	data := make([]interface{}, len(schema))
	for col := range data {
		switch schema[col].DataType {
		case DataTypeBool:
			v := make([]bool, rows)
			for row := 0; row < rows; row++ {
				v[row] = (rng.Int31() % 2) == 0
			}
			data[col] = v
		case DataTypeUint8:
			v := make([]uint8, rows)
			for row := 0; row < rows; row++ {
				v[row] = uint8(rng.Uint32())
			}
			data[col] = v
		case DataTypeUint16:
			v := make([]uint16, rows)
			for row := 0; row < rows; row++ {
				v[row] = uint16(rng.Uint32())
			}
			data[col] = v
		case DataTypeUint32:
			v := make([]uint32, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Uint32()
			}
			data[col] = v
		case DataTypeUint64:
			v := make([]uint64, rows)
			for row := 0; row < rows; row++ {
				v[row] = rng.Uint64()
			}
			data[col] = v
		case DataTypeBytes:
			v := make([][]byte, rows)
			for row := 0; row < rows; row++ {
				v[row] = make([]byte, rng.Intn(20))
				rng.Read(v[row])
			}
			data[col] = v
		case DataTypePrefixBytes:
			v := make([][]byte, rows)
			for row := 0; row < rows; row++ {
				v[row] = make([]byte, rng.Intn(20))
				rng.Read(v[row])
			}
			// PrefixBytes are required to be lexicographically sorted.
			slices.SortFunc(v, bytes.Compare)
			data[col] = v

		}
	}

	var w blockWriter
	w.init(0, 0, schema)

	for row := 0; row < rows; row++ {
		for col := range schema {
			switch schema[col].DataType {
			case DataTypeBool:
				w.PutBitmap(col, data[col].([]bool)[row])
			case DataTypeUint8:
				w.PutUint8(col, data[col].([]uint8)[row])
			case DataTypeUint16:
				w.PutUint16(col, data[col].([]uint16)[row])
			case DataTypeUint32:
				w.PutUint32(col, data[col].([]uint32)[row])
			case DataTypeUint64:
				w.PutUint64(col, data[col].([]uint64)[row])
			case DataTypeBytes:
				w.PutRawBytes(col, data[col].([][]byte)[row])
			case DataTypePrefixBytes:
				w.PutPrefixBytes(col, data[col].([][]byte)[row])
			}
		}
	}

	return w.Finish(uint32(rows)), data
}

func testRandomBlock(t *testing.T, rng *rand.Rand, rows int, schema []ColumnConfig) {
	dataTypes := make(DataTypes, len(schema))
	for i := range schema {
		dataTypes[i] = schema[i].DataType
	}

	t.Run(dataTypes.String(), func(t *testing.T) {
		block, data := randBlock(rng, rows, schema)

		r := NewBlockReader(block, 0)
		if uint32(r.header.Columns) != uint32(len(schema)) {
			t.Fatalf("expected %d columns, but found %d\n", len(schema), r.header.Columns)
		}
		if r.header.Rows != uint32(rows) {
			t.Fatalf("expected %d rows, but found %d\n", rows, r.header.Rows)
		}
		for col := range schema {
			if schema[col].DataType != r.Column(col).Desc.DataType() {
				t.Fatalf("schema mismatch: %s != %s\n", schema[col], r.Column(col).Desc.DataType())
			}
			if schema[col].BundleSize > 0 && r.Column(col).Desc.DataType() != DataTypePrefixBytes {
				t.Fatalf("schema mismatch: col %d has bundle size %d; column has %s data type\n",
					col, schema[col].BundleSize, r.Column(col).DataType)
			}

		}

		for col := range data {
			vec := r.Column(col)
			for i := uint32(0); i < vec.N; i++ {
				if i != uint32(vec.Rank(int(i))) {
					t.Fatalf("expected rank %d, but found %d", i, vec.Rank(int(i)))
				}
			}

			var got interface{}
			switch schema[col].DataType {
			case DataTypeBool:
				b := r.Column(col).Bitmap()
				vals := make([]bool, r.header.Rows)
				for i := range vals {
					vals[i] = b.Get(i)
				}
				got = vals
			case DataTypeUint8:
				got = r.Column(col).UnsafeUint8s().Clone(rows)
			case DataTypeUint16:
				got = r.Column(col).UnsafeUint16s().Clone(rows)
			case DataTypeUint32:
				got = r.Column(col).UnsafeUint32s().Clone(rows)
			case DataTypeUint64:
				got = r.Column(col).UnsafeUint64s().Clone(rows)
			case DataTypeBytes:
				c := r.Column(col)
				vals2 := make([][]byte, rows)
				vals := c.RawBytes()
				for i := range vals2 {
					vals2[i] = vals.At(i)
				}
				got = vals2
			case DataTypePrefixBytes:
				c := r.Column(col)
				vals2 := make([][]byte, rows)
				vals := c.PrefixBytes()
				for i := range vals2 {
					vals2[i] = append(append(append([]byte{}, vals.SharedPrefix()...), vals.BundlePrefix(i)...), vals.RowSuffix(i)...)
				}
				got = vals2
			}
			if !reflect.DeepEqual(data[col], got) {
				t.Fatalf("%d: %s: expected\n%+v\ngot\n%+v\n% x",
					col, schema[col], data[col], got, r.data())
			}
		}
	})
}

func TestBlockWriterRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("Seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))
	randInt := func(lo, hi int) int {
		return lo + rng.Intn(hi-lo)
	}
	testRandomBlock(t, rng, randInt(1, 100), []ColumnConfig{{DataType: DataTypeBool}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnConfig{{DataType: DataTypeUint8}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnConfig{{DataType: DataTypeUint16}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnConfig{{DataType: DataTypeUint32}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnConfig{{DataType: DataTypeUint64}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnConfig{{DataType: DataTypeBytes}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnConfig{{DataType: DataTypePrefixBytes, BundleSize: 16}})

	for i := 0; i < 100; i++ {
		schema := make([]ColumnConfig, 2+rng.Intn(8))
		for j := range schema {
			schema[j].DataType = DataType(randInt(1, int(dataTypesCount)-1))
			if schema[j].DataType == DataTypePrefixBytes {
				schema[j].BundleSize = 1 << randInt(1, 6)
			}
		}
		testRandomBlock(t, rng, randInt(1, 100), schema)
	}
}

func TestBlockWriterNullValues(t *testing.T) {
	var w blockWriter
	const rows = 16
	w.init(0, 0, []ColumnConfig{{DataType: DataTypeUint32}})
	for i := 0; i < rows; i++ {
		if i%2 == 0 {
			w.PutNull(0)
		} else {
			w.PutUint32(0, uint32(i))
		}
	}
	r := NewBlockReader(w.Finish(rows), 0)
	col := r.Column(0)
	for i := 0; i < int(col.N); i++ {
		if j := col.Rank(i); j < 0 {
			if i%2 != 0 {
				t.Fatalf("expected non-NULL value, but found NULL")
			}
		} else if i%2 == 0 {
			t.Fatalf("expected NULL value, but found %d", col.Int32()[j])
		}
	}
}

func BenchmarkBlock(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	blocks := make([][]byte, 128)
	for i := range blocks {
		blocks[i], _ = randBlock(rng, 4096, []ColumnConfig{{DataType: DataTypeUint64}})
	}

	b.Run("not-null", func(b *testing.B) {
		var sum int64
		for i, k := 0, 0; i < b.N; i += k {
			r := NewBlockReader(blocks[rng.Intn(len(blocks))], 0)
			col := r.Column(0)
			vals := col.Int64()

			k = int(col.N)
			if k > b.N-i {
				k = b.N - i
			}
			for j := 0; j < k; j++ {
				sum += vals[j]
			}
		}
		if testing.Verbose() {
			fmt.Fprint(io.Discard, sum)
		}
	})

	b.Run("null-get", func(b *testing.B) {
		var sum int64
		for i, k := 0, 0; i < b.N; i += k {
			r := NewBlockReader(blocks[rng.Intn(len(blocks))], 0)
			col := r.Column(0)
			vals := col.Int64()

			k = int(col.N)
			if k > b.N-i {
				k = b.N - i
			}
			for j := 0; j < k; j++ {
				if !col.Null(j) {
					sum += vals[j]
				}
			}
		}
		if testing.Verbose() {
			fmt.Fprint(io.Discard, sum)
		}
	})

	b.Run("null-rank", func(b *testing.B) {
		var sum int64
		for i, k := 0, 0; i < b.N; i += k {
			r := NewBlockReader(blocks[rng.Intn(len(blocks))], 0)
			col := r.Column(0)
			vals := col.Int64()

			k = int(col.N)
			if k > b.N-i {
				k = b.N - i
			}
			for j := 0; j < k; j++ {
				if r := col.Rank(j); r >= 0 {
					sum += vals[r]
				}
			}
		}
		if testing.Verbose() {
			fmt.Fprint(io.Discard, sum)
		}
	})
}

//go:linkname memmove runtime.memmove
func memmove(from, to unsafe.Pointer, n uintptr)

//go:linkname mallocgc runtime.mallocgc
func mallocgc(size uintptr, typ unsafe.Pointer, needzero bool) unsafe.Pointer
