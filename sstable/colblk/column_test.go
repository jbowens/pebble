// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/stretchr/testify/require"
)

func TestColumnDesc(t *testing.T) {
	// Ensure that a simple roundtrip from DataType -> ColumnDesc ->
	// DataType arrives back at the original DataType.
	for dt := DataTypeInvalid; dt < dataTypesCount; dt++ {
		require.Equal(t, dt, ColumnDesc(dt).DataType())
	}

	type testCase struct {
		desc       ColumnDesc
		dataType   DataType
		enc        ColumnEncoding
		nullBitmap bool
		str        string
	}
	testCases := []testCase{
		{
			desc:       ColumnDesc(DataTypeBool),
			dataType:   DataTypeBool,
			str:        "bool",
			nullBitmap: false,
			enc:        EncodingDefault,
		},
		{
			desc:       ColumnDesc(DataTypeUint16),
			dataType:   DataTypeUint16,
			enc:        EncodingDefault,
			nullBitmap: false,
			str:        "int16",
		},
		{
			desc:       ColumnDesc(DataTypeUint64).WithEncoding(EncodingConstant).WithNullBitmap(),
			dataType:   DataTypeUint64,
			enc:        EncodingConstant,
			nullBitmap: true,
			str:        "int64+nullbitmap+constant",
		},
		{
			desc:       ColumnDesc(DataTypePrefixBytes),
			dataType:   DataTypePrefixBytes,
			nullBitmap: false,
			str:        "prefixbytes",
		},
		{
			desc:       ColumnDesc(DataTypeBytes).WithEncoding(EncodingConstant),
			dataType:   DataTypeBytes,
			enc:        EncodingConstant,
			nullBitmap: false,
			str:        "bytes+constant",
		},
		{
			desc:       ColumnDesc(DataTypeBytes),
			dataType:   DataTypeBytes,
			enc:        EncodingDefault,
			nullBitmap: false,
			str:        "bytes",
		},
		{
			desc:       ColumnDesc(DataTypeUint64).WithEncoding(EncodingDeltaInt16),
			dataType:   DataTypeUint64,
			enc:        EncodingDeltaInt16,
			nullBitmap: false,
			str:        "int64+delta16",
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%08b", tc.desc), func(t *testing.T) {
			if got := tc.desc.String(); got != tc.str {
				t.Errorf("String() = %q; want %q", got, tc.str)
			}
			if got := tc.desc.DataType(); got != tc.dataType {
				t.Errorf("DataType() = %s; want %s", got, tc.dataType)
			}
			if got := tc.desc.Encoding(); got != tc.enc {
				t.Errorf("Encoding() = %s; want %s", got, tc.enc)
			}
			if got := tc.desc.HasNullBitmap(); got != tc.nullBitmap {
				t.Errorf("HasNullBitmap() = %t; want %t", got, tc.nullBitmap)
			}
		})
	}
}

func dataTypeFromName(name string) DataType {
	for dt, n := range dataTypeName {
		if n == name {
			return DataType(dt)
		}
	}
	return DataTypeInvalid
}

type uintColumnWriter interface {
	ColumnWriter
	Init(UintDefault)
}

func TestUints(t *testing.T) {
	var b8 Uint8Builder
	var b16 Uint16Builder
	var b32 Uint32Builder
	var b64 Uint64Builder

	var out bytes.Buffer
	var widths []int
	var writers []uintColumnWriter
	datadriven.RunTest(t, "testdata/uints", func(t *testing.T, td *datadriven.TestData) string {
		out.Reset()
		switch td.Cmd {
		case "init":
			widths = widths[:0]
			writers = writers[:0]
			td.ScanArgs(t, "widths", &widths)
			var defaultConfig UintDefault
			if td.HasArg("default-zero") {
				defaultConfig = UintDefaultZero
			}
			for _, w := range widths {
				switch w {
				case 8:
					writers = append(writers, &b8)
				case 16:
					writers = append(writers, &b16)
				case 32:
					writers = append(writers, &b32)
				case 64:
					writers = append(writers, &b64)
				default:
					panic(fmt.Sprintf("unknown width: %d", w))
				}
				fmt.Fprintf(&out, "b%d\n", w)
				writers[len(writers)-1].Init(defaultConfig)
			}
			return out.String()
		case "write":
			for _, f := range strings.Fields(td.Input) {
				delim := strings.IndexByte(f, ':')
				i, err := strconv.Atoi(f[:delim])
				if err != nil {
					return err.Error()
				}
				for _, width := range widths {
					v, err := strconv.ParseUint(f[delim+1:], 10, width)
					if err != nil {
						return err.Error()
					}
					switch width {
					case 8:
						b8.Set(i, uint8(v))
					case 16:
						b16.Set(i, uint16(v))
					case 32:
						b32.Set(i, uint32(v))
					case 64:
						b64.Set(i, v)
					default:
						panic(fmt.Sprintf("unknown width: %d", width))
					}
				}
			}
			return out.String()
		case "size":
			var rowCounts []int
			td.ScanArgs(t, "rows", &rowCounts)
			for wIdx, w := range writers {
				fmt.Fprintf(&out, "b%d:\n", widths[wIdx])
				for _, rows := range rowCounts {
					fmt.Fprintf(&out, "  %d: %T.Size(%d, 0) = %d\n", widths[wIdx], w, rows, w.Size(rows, 0))
				}
			}
			return out.String()
		case "finish":
			var rows int
			var finishWidths []int
			td.ScanArgs(t, "rows", &rows)
			td.ScanArgs(t, "widths", &finishWidths)
			var newWriters []uintColumnWriter
			var newWidths []int
			for wIdx, width := range widths {
				var shouldFinish bool
				for _, fw := range finishWidths {
					shouldFinish = shouldFinish || width == fw
				}
				if shouldFinish {
					buf := make([]byte, writers[wIdx].Size(rows, 0))
					off, desc := writers[wIdx].Finish(0, rows, 0, buf)
					fmt.Fprintf(&out, "b%d: %T:\n", width, writers[wIdx])
					f := binfmt.New(buf).LineWidth(20)
					vec := makeVec(rows, desc, f.Pointer(0), 0, off)
					uintsToBinFormatter(f, rows, vec)
					fmt.Fprintf(&out, "%s", f.String())
				} else {
					fmt.Fprintf(&out, "Keeping b%d open\n", width)
					newWidths = append(newWidths, width)
					newWriters = append(newWriters, writers[wIdx])
				}
			}
			writers = newWriters
			widths = newWidths
			return out.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", td.Cmd))
		}
	})
}
