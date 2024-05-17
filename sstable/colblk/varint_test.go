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
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/binfmt"
)

func TestVarint32(t *testing.T) {
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/varint32", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "build":
			var builder uvarint32Builder
			builder.Reset()
			for _, s := range strings.Fields(td.Input) {
				v, err := strconv.ParseUint(s, 10, 32)
				if err != nil {
					return err.Error()
				}
				builder.Append(uint32(v))
			}
			dst := make([]byte, builder.MaxSize(0))
			off := builder.Finish(0, dst)
			dst = dst[:off]
			fmt.Fprintln(&buf, "Encoded:")
			hexDump(&buf, dst)

			vc := Uvarint32{
				varints: makeRawBytes(uint32(builder.bb.nKeys), unsafe.Pointer(unsafe.SliceData(dst[0:off]))),
			}
			fmt.Fprintln(&buf)
			f := binfmt.New(dst[0:off]).LineWidth(40)
			rawBytesToBinFormatter(f, uint32(builder.bb.nKeys), func(b []byte) string {
				if len(b) == 0 {
					return "(0, 0, 0, 0)"
				}
				var dst [4]uint32
				decodeGroupVarint32(dst[:], b)
				s := fmt.Sprintf("(%d, %d, %d, %d)", dst[0], dst[1], dst[2], dst[3])
				return s
			})
			fmt.Fprint(&buf, f.String())
			fmt.Fprint(&buf, "Decoded:")
			for i := 0; i < int(builder.n); i++ {
				if i%10 == 0 {
					fmt.Fprint(&buf, "\n")
				}
				fmt.Fprintf(&buf, "  %04d", vc.Get(i))
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
}
