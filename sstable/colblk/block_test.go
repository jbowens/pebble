// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"golang.org/x/exp/rand"
)

func TestBlockWriter(t *testing.T) {
	panicIfErr := func(dataType DataType, stringValue string, err error) {
		if err != nil {
			panicf("unable to decode %q as value for data type %s: %s", stringValue, dataType, err)
		}
	}
	var buf bytes.Buffer
	var rows int
	var colConfigs []ColumnConfig
	var colWriters []ColumnWriter
	datadriven.RunTest(t, "testdata/block_writer", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "init":
			colTypeNames, ok := td.Arg("schema")
			if !ok {
				return "schema argument missing"
			}
			colConfigs = make([]ColumnConfig, len(colTypeNames.Vals))
			colWriters = make([]ColumnWriter, len(colConfigs))
			for i, v := range colTypeNames.Vals {
				colConfigs[i].DataType = dataTypeFromName(v)
				if colConfigs[i].DataType == DataTypePrefixBytes {
					colConfigs[i].BundleSize = 16
				}
				switch colConfigs[i].DataType {
				case DataTypeBool:
					colWriters[i] = &BitmapBuilder{}
				case DataTypeUint8:
					colWriters[i] = &Uint8Builder{}
				case DataTypeUint16:
					colWriters[i] = &Uint16Builder{}
				case DataTypeUint32:
					colWriters[i] = &Uint32Builder{}
				case DataTypeUint64:
					colWriters[i] = &Uint64Builder{}
				case DataTypeBytes:
					bb := &BytesBuilder{}
					bb.Init(0)
					colWriters[i] = bb
				case DataTypePrefixBytes:
					bb := &BytesBuilder{}
					bb.Init(colConfigs[i].BundleSize)
					colWriters[i] = bb
				default:
					panicf("unsupported data type: %s", colConfigs[i].DataType)
				}
				colWriters[i].Reset()
			}
			rows = 0
			return ""
		case "write":
			lines := strings.Split(td.Input, "\n")
			lineFields := make([][]string, len(lines))
			for i, line := range lines {
				lineFields[i] = strings.Fields(line)
			}
			rows += len(lineFields)
			for c, cfg := range colConfigs {
				switch cfg.DataType {
				case DataTypeBool:
					bb := colWriters[c].(*BitmapBuilder)
					for r := range lineFields {
						v, err := strconv.ParseBool(lineFields[r][c])
						panicIfErr(cfg.DataType, lineFields[r][c], err)
						(*bb) = (*bb).Set(r, v)
					}
					colWriters[c] = bb
				case DataTypeUint8:
					b := colWriters[c].(*Uint8Builder)
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 8)
						panicIfErr(cfg.DataType, lineFields[r][c], err)
						b.Set(r, uint8(v))
					}
				case DataTypeUint16:
					b := colWriters[c].(*Uint16Builder)
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 16)
						panicIfErr(cfg.DataType, lineFields[r][c], err)
						b.Set(r, uint16(v))
					}
				case DataTypeUint32:
					b := colWriters[c].(*Uint32Builder)
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 32)
						panicIfErr(cfg.DataType, lineFields[r][c], err)
						b.Set(r, uint32(v))
					}
				case DataTypeUint64:
					b := colWriters[c].(*Uint64Builder)
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 64)
						panicIfErr(cfg.DataType, lineFields[r][c], err)
						b.Set(r, v)
					}
				case DataTypeBytes:
					b := colWriters[c].(*BytesBuilder)
					for r := range lineFields {
						b.Put([]byte(lineFields[r][c]))
					}
				case DataTypePrefixBytes:
					b := colWriters[c].(*BytesBuilder)
					for r := range lineFields {
						var shared int
						if r > 0 {
							shared = bytesSharedPrefix([]byte(lineFields[r-1][c]), []byte(lineFields[r][c]))
						}
						b.PutOrdered([]byte(lineFields[r][c]), shared)
					}
				default:
					panicf("unsupported data type: %s", cfg.DataType)
				}
			}
			return ""
		case "finish":
			block := FinishBlock(int(rows), colWriters)
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
	buf := buildBlock(schema, rows, data)
	return buf, data
}

func buildBlock(schema []ColumnConfig, rows int, data []interface{}) []byte {
	buf := make([]byte, blockHeaderSize(len(schema), 0))
	WriteHeader(buf, Header{Columns: uint16(len(schema)), Rows: uint32(rows)})
	pageOffset := uint32(len(buf))
	for col := range schema {
		var cw ColumnWriter
		switch schema[col].DataType {
		case DataTypeBool:
			var bb BitmapBuilder
			for row, v := range data[col].([]bool) {
				bb = bb.Set(row, v)
			}
			cw = &bb
		case DataTypeUint8:
			var b Uint8Builder
			for row, v := range data[col].([]uint8) {
				b.Set(row, v)
			}
			cw = &b
		case DataTypeUint16:
			var b Uint16Builder
			for row, v := range data[col].([]uint16) {
				b.Set(row, v)
			}
			cw = &b
		case DataTypeUint32:
			var b Uint32Builder
			for row, v := range data[col].([]uint32) {
				b.Set(row, v)
			}
			cw = &b
		case DataTypeUint64:
			var b Uint64Builder
			for row, v := range data[col].([]uint64) {
				b.Set(row, v)
			}
			cw = &b
		case DataTypeBytes:
			var b BytesBuilder
			for _, v := range data[col].([][]byte) {
				b.Put(v)
			}
			cw = &b
		case DataTypePrefixBytes:
			var b BytesBuilder
			b.Init(16)
			s := data[col].([][]byte)
			for i, v := range s {
				var shared int
				if i > 0 {
					shared = bytesSharedPrefix(s[i-1], v)
				}
				b.PutOrdered(v, shared)
			}
			cw = &b
		}
		if newOffset := cw.Size(rows, pageOffset); uint32(len(buf)) < newOffset {
			newBuf := make([]byte, newOffset)
			copy(newBuf, buf)
			buf = newBuf
		}

		hi := blockHeaderSize(col, 0)
		binary.LittleEndian.PutUint32(buf[hi+1:], uint32(pageOffset))
		var desc ColumnDesc
		pageOffset, desc = cw.Finish(0, rows, pageOffset, buf)
		buf[hi] = byte(desc)
	}
	buf = append(buf, 0x00)
	return buf
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
					vals2[i] = append(append(append([]byte{}, vals.SharedPrefix()...), vals.RowBundlePrefix(i)...), vals.RowSuffix(i)...)
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

func TestNullable(t *testing.T) {
	var w Nullable[*Uint32Builder] = MakeNullable(DataTypeUint32, new(Uint32Builder))
	w.writer.Init(UintDefaultNone)

	const rows = 16
	for i := 0; i < rows; i++ {
		if i%2 == 0 {
			w.SetNull(i)
		} else {
			builder, j := w.NotNull(i)
			builder.Set(j, uint32(i))
		}
	}
	block := FinishBlock(rows, []ColumnWriter{&w})
	r := NewBlockReader(block, 0)

	f := binfmt.New(r.data()).LineWidth(20)
	r.headerToBinFormatter(f)
	r.columnToBinFormatter(f, 0, rows)
	t.Log(f.String())

	col := r.Column(0)
	for i := 0; i < int(col.N); i++ {
		if j := col.Rank(i); j < 0 {
			if i%2 != 0 {
				t.Fatalf("%d: expected non-NULL value, but found NULL", i)
			}
		} else if i%2 == 0 {
			t.Fatalf("%d: expected NULL value, but found %d at rank %d", i, col.Int32()[j], j)
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
