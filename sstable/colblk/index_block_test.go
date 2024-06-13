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
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/stretchr/testify/require"
)

func TestIndexBlock(t *testing.T) {
	var r IndexReader
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/index_block", func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()
		switch d.Cmd {
		case "build-first-level":
			var w IndexBlockWriter
			w.Init()
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				endOffset, err := strconv.ParseUint(fields[1], 10, 64)
				require.NoError(t, err)
				l, err := strconv.ParseUint(fields[2], 10, 64)
				require.NoError(t, err)
				w.AddDataBlockHandle([]byte(fields[0]), endOffset, l)
			}
			data := w.Finish()
			r.Init(data)
			return r.DebugString()
		case "build-second-level":
			var w IndexBlockWriter
			w.Init()
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				endOffset, err := strconv.ParseUint(fields[1], 10, 64)
				require.NoError(t, err)
				l, err := strconv.ParseUint(fields[2], 10, 64)
				require.NoError(t, err)
				w.AddIndexBlockHandle([]byte(fields[0]), endOffset, l)
			}
			data := w.Finish()
			r.Init(data)
			return r.DebugString()
		case "iter":
			var it IndexIter
			it.Init(&r)
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				var valid bool
				switch fields[0] {
				case "seek-ge":
					valid = it.SeekGE([]byte(fields[1]))
				case "first":
					valid = it.First()
				case "last":
					valid = it.Last()
				case "next":
					valid = it.Next()
				case "prev":
					valid = it.Prev()
				default:
					panic(fmt.Sprintf("unknown command: %s", fields[0]))
				}
				if valid {
					var h block.Handle
					if it.r.hasExplicitLens {
						h = it.BlockHandle()
						fmt.Fprintf(&buf, "index block %d: %d-%d\n", it.row, h.Offset, h.Offset+h.Length)
					} else {
						h = it.DataBlockHandle()
						fmt.Fprintf(&buf, "data block %d: %d-%d\n", it.row, h.Offset, h.Offset+h.Length)
					}
				} else {
					fmt.Fprintln(&buf, ".")
				}
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unknown command: %s", d.Cmd))
		}
	})
}
