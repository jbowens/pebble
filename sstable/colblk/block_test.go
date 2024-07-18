// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"golang.org/x/exp/rand"
)

type ColumnSpec struct {
	DataType
	// ZeroAsAbsent is used to determine whether a zero value should be
	// represented using the "presence" encoding, possibly encoding a
	// PresenceBitmap.
	ZeroAsAbsent bool
	BundleSize   int // only used for PrefixBytes.
}

func TestBlockWriter(t *testing.T) {
	panicIfErr := func(dataType DataType, stringValue string, err error) {
		if err != nil {
			panic(fmt.Sprintf("unable to decode %q as value for data type %s: %s", stringValue, dataType, err))
		}
	}
	var buf bytes.Buffer
	var rows int
	var colDataTypes []DataType
	var colWriters []ColumnWriter
	datadriven.RunTest(t, "testdata/block_writer", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "init":
			colTypeNames, ok := td.Arg("schema")
			if !ok {
				return "schema argument missing"
			}
			colDataTypes = make([]DataType, len(colTypeNames.Vals))
			colWriters = make([]ColumnWriter, len(colDataTypes))
			for i, v := range colTypeNames.Vals {
				colDataTypes[i] = dataTypeFromName(v)
				switch colDataTypes[i] {
				case DataTypeBool:
					colWriters[i] = &BitmapBuilder{}
				case DataTypeUint8:
					b := &UintBuilder[uint8]{}
					b.Init()
					colWriters[i] = b
				case DataTypeUint16:
					b := &UintBuilder[uint16]{}
					b.Init()
					colWriters[i] = b
				case DataTypeUint32:
					b := &UintBuilder[uint32]{}
					b.Init()
					colWriters[i] = b
				case DataTypeUint64:
					b := &UintBuilder[uint64]{}
					b.Init()
					colWriters[i] = b
				case DataTypeBytes:
					bb := &RawBytesBuilder{}
					bb.Reset()
					colWriters[i] = bb
				case DataTypePrefixBytes:
					panic("unimplemented")
				default:
					panic(fmt.Sprintf("unsupported data type: %s", v))
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
			for c, dataType := range colDataTypes {
				switch dataType {
				case DataTypeBool:
					bb := colWriters[c].(*BitmapBuilder)
					for r := range lineFields {
						v, err := strconv.ParseBool(lineFields[r][c])
						panicIfErr(dataType, lineFields[r][c], err)
						bb.Set(r, v)
					}
				case DataTypeUint8:
					b := colWriters[c].(*UintBuilder[uint8])
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 8)
						panicIfErr(dataType, lineFields[r][c], err)
						b.Set(r, uint8(v))
					}
				case DataTypeUint16:
					b := colWriters[c].(*UintBuilder[uint16])
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 16)
						panicIfErr(dataType, lineFields[r][c], err)
						b.Set(r, uint16(v))
					}
				case DataTypeUint32:
					b := colWriters[c].(*UintBuilder[uint32])
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 32)
						panicIfErr(dataType, lineFields[r][c], err)
						b.Set(r, uint32(v))
					}
				case DataTypeUint64:
					b := colWriters[c].(*UintBuilder[uint64])
					for r := range lineFields {
						v, err := strconv.ParseUint(lineFields[r][c], 10, 64)
						panicIfErr(dataType, lineFields[r][c], err)
						b.Set(r, v)
					}
				case DataTypeBytes:
					b := colWriters[c].(*RawBytesBuilder)
					for r := range lineFields {
						b.Put([]byte(lineFields[r][c]))
					}
				case DataTypePrefixBytes:
					panic("unimplemented")
				default:
					panic(fmt.Sprintf("unsupported data type: %s", dataType))
				}
			}
			return ""
		case "finish":
			block := FinishBlock(int(rows), colWriters)
			r := ReadBlock(block, 0)
			return r.FormattedString()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func dataTypeFromName(name string) DataType {
	for dt, n := range dataTypeName {
		if n == name {
			return DataType(dt)
		}
	}
	return DataTypeInvalid
}

// randBlock generates a random block of n rows with the provided schema. It
// returns the serialized raw block and a []interface{} slice containing the
// generated data. The type of each element of the slice is dependent on the
// corresponding column's type.
func randBlock(rng *rand.Rand, rows int, schema []ColumnSpec) ([]byte, []interface{}) {
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
				l := rng.Intn(20)
				if !schema[col].ZeroAsAbsent {
					// PrefixBytes does not support empty slices.
					l++
				}
				v[row] = make([]byte, l)
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

func writeUintZeroAsAbsent[U Uint](vals []U) ColumnWriter {
	var b UintBuilder[U]
	b.Init()
	w := &DefaultAbsent[*UintBuilder[U]]{Writer: &b}
	for row, v := range vals {
		if v != 0 {
			w, j := w.Present(row)
			w.Set(j, v)
		}
	}
	return w
}

func writeUintCol[U Uint](vals []U) ColumnWriter {
	var b UintBuilder[U]
	b.Init()
	for row, v := range vals {
		b.Set(row, v)
	}
	return &b
}

func buildBlock(schema []ColumnSpec, rows int, data []interface{}) []byte {
	cw := make([]ColumnWriter, len(schema))
	for col := range schema {
		switch schema[col].DataType {
		case DataTypeBool:
			if schema[col].ZeroAsAbsent {
				panic("not supported")
			}
			var bb BitmapBuilder
			bb.Reset()
			for row, v := range data[col].([]bool) {
				bb.Set(row, v)
			}
			cw[col] = &bb
		case DataTypeUint8:
			if schema[col].ZeroAsAbsent {
				cw[col] = writeUintZeroAsAbsent(data[col].([]uint8))
			} else {
				cw[col] = writeUintCol(data[col].([]uint8))
			}
		case DataTypeUint16:
			if schema[col].ZeroAsAbsent {
				cw[col] = writeUintZeroAsAbsent(data[col].([]uint16))
			} else {
				cw[col] = writeUintCol(data[col].([]uint16))
			}
		case DataTypeUint32:
			if schema[col].ZeroAsAbsent {
				cw[col] = writeUintZeroAsAbsent(data[col].([]uint32))
			} else {
				cw[col] = writeUintCol(data[col].([]uint32))
			}
		case DataTypeUint64:
			if schema[col].ZeroAsAbsent {
				cw[col] = writeUintZeroAsAbsent(data[col].([]uint64))
			} else {
				cw[col] = writeUintCol(data[col].([]uint64))
			}
		case DataTypeBytes:
			var b RawBytesBuilder
			b.Reset()
			if schema[col].ZeroAsAbsent {
				w := &DefaultAbsent[*RawBytesBuilder]{Writer: &b}
				for _, v := range data[col].([][]byte) {
					if len(v) > 0 {
						b.Put(v)
					}
				}
				cw[col] = w
			} else {
				for _, v := range data[col].([][]byte) {
					b.Put(v)
				}
				cw[col] = &b
			}
		case DataTypePrefixBytes:
			pbb := new(PrefixBytesBuilder)
			pbb.Init(schema[col].BundleSize)
			colData := data[col].([][]byte)
			if schema[col].ZeroAsAbsent {
				panic("todo")
				w := &DefaultAbsent[*PrefixBytesBuilder]{Writer: pbb}
				for r, v := range colData {
					if len(v) > 0 {
						sharedPrefix := 0
						if r > 0 {
							sharedPrefix = bytesSharedPrefix(colData[r-1], v)
						}
						innerWriter, _ := w.Present(r)
						innerWriter.Put(v, sharedPrefix)
					}
				}
				cw[col] = w
			} else {
				for r, v := range colData {
					sharedPrefix := 0
					if r > 0 {
						sharedPrefix = bytesSharedPrefix(colData[r-1], v)
					}
					pbb.Put(v, sharedPrefix)
				}
				cw[col] = pbb
			}
		}
	}
	return FinishBlock(rows, cw)
}

func testRandomBlock(t *testing.T, rng *rand.Rand, rows int, schema []ColumnSpec) {
	var sb strings.Builder
	for i := range schema {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(schema[i].String())
	}

	t.Run(sb.String(), func(t *testing.T) {
		block, data := randBlock(rng, rows, schema)
		fmt.Printf("Generated random block: %x\n", block)
		r := ReadBlock(block, 0)
		if uint32(r.header.Columns) != uint32(len(schema)) {
			t.Fatalf("expected %d columns, but found %d\n", len(schema), r.header.Columns)
		}
		if r.header.Rows != uint32(rows) {
			t.Fatalf("expected %d rows, but found %d\n", rows, r.header.Rows)
		}
		for col := range schema {
			if schema[col].DataType != r.DataType(col) {
				t.Fatalf("schema mismatch: %s != %s\n", schema[col], r.DataType(col))
			}
		}

		for col := range data {
			var got interface{}
			spec := schema[col]
			switch spec.DataType {
			case DataTypeBool:
				b := DecodeColumn(&r, col, spec.DataType, DecodeBitmap)
				got = readEntireColumn(&b, rows)
			case DataTypeUint8:
				b := DecodeColumn(&r, col, spec.DataType,
					decodeColumnMaybeWithPresence(spec, DecodeUnsafeIntegerSlice[uint8], 0))
				got = readEntireColumn(b, rows)
			case DataTypeUint16:
				b := DecodeColumn(&r, col, spec.DataType,
					decodeColumnMaybeWithPresence(spec, DecodeUnsafeIntegerSlice[uint16], 0))
				got = readEntireColumn(b, rows)
			case DataTypeUint32:
				b := DecodeColumn(&r, col, spec.DataType,
					decodeColumnMaybeWithPresence(spec, DecodeUnsafeIntegerSlice[uint32], 0))
				got = readEntireColumn(b, rows)
			case DataTypeUint64:
				b := DecodeColumn(&r, col, spec.DataType,
					decodeColumnMaybeWithPresence(spec, DecodeUnsafeIntegerSlice[uint64], 0))
				got = readEntireColumn(b, rows)
			case DataTypeBytes:
				b := DecodeColumn(&r, col, spec.DataType,
					decodeColumnMaybeWithPresence(spec, DecodeRawBytes, nil))
				got = readEntireColumn(b, rows)
			case DataTypePrefixBytes:
				b := DecodeColumn(&r, col, spec.DataType,
					decodeColumnMaybeWithPresence(spec, DecodePrefixBytes, nil))
				got = readEntireColumn(b, rows)
			}
			if !reflect.DeepEqual(data[col], got) {
				t.Fatalf("%d: %s: expected\n%+v\ngot\n%+v\n% x",
					col, schema[col], data[col], got, r.data)
			}
		}
	})
}

func decodeColumnMaybeWithPresence[V any, R ColumnReader[V]](
	spec ColumnSpec, decodeFunc DecodeFunc[R], defaultValue V,
) DecodeFunc[ColumnReader[V]] {
	return func(b []byte, off uint32, rows int) (ColumnReader[V], uint32) {
		if spec.ZeroAsAbsent {
			return DecodePresenceWithDefault(defaultValue, decodeFunc)(b, off, rows)
		}
		return decodeFunc(b, off, rows)
	}
}

func readEntireColumn[T any](r ColumnReader[T], rows int) []T {
	vals := make([]T, rows)
	for i := 0; i < rows; i++ {
		vals[i] = r.At(i)
	}
	return vals
}

func TestBlockWriterRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	seed = 1721328733647796000
	t.Logf("Seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))
	randInt := func(lo, hi int) int {
		return lo + rng.Intn(hi-lo)
	}
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeBool}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint8}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint16}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint32}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeUint64}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypeBytes}})
	testRandomBlock(t, rng, randInt(1, 100), []ColumnSpec{{DataType: DataTypePrefixBytes, BundleSize: 4}})

	for i := 0; i < 100; i++ {
		schema := make([]ColumnSpec, 2+rng.Intn(8))
		for j := range schema {
			schema[j].DataType = DataType(randInt(1, int(dataTypesCount)))
			if schema[j].DataType == DataTypePrefixBytes {
				schema[j].BundleSize = 1 << randInt(1, 7)
			}
			/*
				switch schema[j].DataType {
				case DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64, DataTypeBytes, DataTypePrefixBytes:
					schema[j].ZeroAsAbsent = rng.Intn(2) == 0
				}
			*/
		}
		testRandomBlock(t, rng, randInt(1, 100), schema)
	}
}
