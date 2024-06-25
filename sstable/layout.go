// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/rowblk"
)

// Layout describes the block organization of an sstable.
type Layout struct {
	// NOTE: changes to fields in this struct should also be reflected in
	// ValidateBlockChecksums, which validates a static list of BlockHandles
	// referenced in this struct.

	Data       []BlockHandleWithProperties
	Index      []BlockHandle
	TopIndex   BlockHandle
	Filter     BlockHandle
	RangeDel   BlockHandle
	RangeKey   BlockHandle
	ValueBlock []BlockHandle
	ValueIndex BlockHandle
	Properties BlockHandle
	MetaIndex  BlockHandle
	Footer     BlockHandle
	Format     TableFormat
}

// Describe returns a description of the layout. If the verbose parameter is
// true, details of the structure of each block are returned as well.
func (l *Layout) Describe(
	w io.Writer, verbose bool, r *Reader, fmtRecord func(key *base.InternalKey, value []byte),
) {
	ctx := context.TODO()
	type namedBlockHandle struct {
		BlockHandle
		name string
	}
	var blocks []namedBlockHandle

	for i := range l.Data {
		blocks = append(blocks, namedBlockHandle{l.Data[i].BlockHandle, "data"})
	}
	for i := range l.Index {
		blocks = append(blocks, namedBlockHandle{l.Index[i], "index"})
	}
	if l.TopIndex.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.TopIndex, "top-index"})
	}
	if l.Filter.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.Filter, "filter"})
	}
	if l.RangeDel.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.RangeDel, "range-del"})
	}
	if l.RangeKey.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.RangeKey, "range-key"})
	}
	for i := range l.ValueBlock {
		blocks = append(blocks, namedBlockHandle{l.ValueBlock[i], "value-block"})
	}
	if l.ValueIndex.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.ValueIndex, "value-index"})
	}
	if l.Properties.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.Properties, "properties"})
	}
	if l.MetaIndex.Length != 0 {
		blocks = append(blocks, namedBlockHandle{l.MetaIndex, "meta-index"})
	}
	if l.Footer.Length != 0 {
		if l.Footer.Length == levelDBFooterLen {
			blocks = append(blocks, namedBlockHandle{l.Footer, "leveldb-footer"})
		} else {
			blocks = append(blocks, namedBlockHandle{l.Footer, "footer"})
		}
	}

	slices.SortFunc(blocks, func(a, b namedBlockHandle) int {
		return cmp.Compare(a.Offset, b.Offset)
	})
	for i := range blocks {
		b := &blocks[i]
		fmt.Fprintf(w, "%10d  %s (%d)\n", b.Offset, b.name, b.Length)

		if !verbose {
			continue
		}
		if b.name == "filter" {
			continue
		}

		if b.name == "footer" || b.name == "leveldb-footer" {
			trailer, offset := make([]byte, b.Length), b.Offset
			_ = r.readable.ReadAt(ctx, trailer, int64(offset))

			if b.name == "footer" {
				checksumType := block.ChecksumType(trailer[0])
				fmt.Fprintf(w, "%10d    checksum type: %s\n", offset, checksumType)
				trailer, offset = trailer[1:], offset+1
			}

			metaHandle, n := binary.Uvarint(trailer)
			metaLen, m := binary.Uvarint(trailer[n:])
			fmt.Fprintf(w, "%10d    meta: offset=%d, length=%d\n", offset, metaHandle, metaLen)
			trailer, offset = trailer[n+m:], offset+uint64(n+m)

			indexHandle, n := binary.Uvarint(trailer)
			indexLen, m := binary.Uvarint(trailer[n:])
			fmt.Fprintf(w, "%10d    index: offset=%d, length=%d\n", offset, indexHandle, indexLen)
			trailer, offset = trailer[n+m:], offset+uint64(n+m)

			fmt.Fprintf(w, "%10d    [padding]\n", offset)

			trailing := 12
			if b.name == "leveldb-footer" {
				trailing = 8
			}

			offset += uint64(len(trailer) - trailing)
			trailer = trailer[len(trailer)-trailing:]

			if b.name == "footer" {
				version := trailer[:4]
				fmt.Fprintf(w, "%10d    version: %d\n", offset, binary.LittleEndian.Uint32(version))
				trailer, offset = trailer[4:], offset+4
			}

			magicNumber := trailer
			fmt.Fprintf(w, "%10d    magic number: 0x%x\n", offset, magicNumber)

			continue
		}

		h, err := r.readBlock(
			context.Background(), b.BlockHandle, nil /* transform */, nil /* readHandle */, nil /* stats */, nil /* iterStats */, nil /* buffer pool */)
		if err != nil {
			fmt.Fprintf(w, "  [err: %s]\n", err)
			continue
		}

		formatTrailer := func() {
			var t block.Trailer
			offset := int64(b.Offset + b.Length)
			_ = r.readable.ReadAt(ctx, t[:], offset)
			bt, checksum := block.DecodeTrailer(t)
			fmt.Fprintf(w, "%10d    [trailer compression=%s checksum=0x%04x]\n", offset, blockType(bt), checksum)
		}

		var lastKey InternalKey
		switch b.name {
		case "data", "range-del", "range-key":
			it, _ := rowblk.NewIter(r.Compare, r.Split, h.Get(), NoTransforms, r.tableFormat.UsesValuePrefix())
			rowblk.Describe(w, b.BlockHandle.Offset, it, func(w io.Writer, kv *base.InternalKV, enc rowblk.KVEncoding) {
				if fmtRecord == nil {
					return
				}
				if l.Format < TableFormatPebblev3 {
					fmtRecord(&kv.K, kv.InPlaceValue())
					return
				}
				// InPlaceValue() will succeed even for data blocks where the
				// actual value is in a different location, since this value was
				// fetched from a blockIter which does not know about value
				// blocks.
				v := kv.InPlaceValue()
				if kv.K.Kind() != InternalKeyKindSet {
					fmtRecord(&kv.K, v)
				} else if !block.ValuePrefix(v[0]).IsValueHandle() {
					fmtRecord(&kv.K, v[1:])
				} else {
					vh := decodeValueHandle(v[1:])
					fmtRecord(&kv.K, []byte(fmt.Sprintf("value handle %+v", vh)))
				}

				if base.InternalCompare(r.Compare, lastKey, kv.K) >= 0 {
					fmt.Fprintf(w, "              WARNING: OUT OF ORDER KEYS!\n")
				}
				lastKey.Trailer = kv.K.Trailer
				lastKey.UserKey = append(lastKey.UserKey[:0], kv.K.UserKey...)
			})
			formatTrailer()
		case "index", "top-index":
			it, _ := rowblk.NewIter(r.Compare, r.Split, h.Get(), NoTransforms, false /* usesValuePrefix */)
			rowblk.Describe(w, b.BlockHandle.Offset, it, func(w io.Writer, kv *base.InternalKV, enc rowblk.KVEncoding) {
				bh, err := decodeBlockHandleWithProperties(kv.InPlaceValue())
				if err != nil {
					fmt.Fprintf(w, "%10d    [err: %s]\n", b.Offset+uint64(enc.Offset), err)
					return
				}
				fmt.Fprintf(w, "%10d    block:%d/%d",
					b.Offset+uint64(enc.Offset), bh.Offset, bh.Length)
			})
			formatTrailer()
		case "properties":
			it, _ := rowblk.NewRawIter(r.Compare, h.Get())
			rowblk.DescribeRaw(w, b.Offset, it, func(w io.Writer, kv *base.InternalKV, enc rowblk.KVEncoding) {
				fmt.Fprintf(w, "%10d    %s (%d)",
					b.Offset+uint64(enc.Offset), kv.K.UserKey, enc.TotalRecordLen)
			})
			formatTrailer()
		case "meta-index":
			it, _ := rowblk.NewRawIter(r.Compare, h.Get())
			rowblk.DescribeRaw(w, b.Offset, it, func(w io.Writer, kv *base.InternalKV, enc rowblk.KVEncoding) {
				value := kv.V.InPlaceValue()
				var bh BlockHandle
				var n int
				var vbih valueBlocksIndexHandle
				isValueBlocksIndexHandle := false
				if bytes.Equal(kv.K.UserKey, []byte(metaValueIndexName)) {
					vbih, n, err = decodeValueBlocksIndexHandle(value)
					bh = vbih.h
					isValueBlocksIndexHandle = true
				} else {
					bh, n = decodeBlockHandle(value)
				}
				if n == 0 || n != len(value) {
					fmt.Fprintf(w, "%10d    [err: %s]\n", b.Offset+uint64(enc.Offset), err)
					return
				}
				var vbihStr string
				if isValueBlocksIndexHandle {
					vbihStr = fmt.Sprintf(" value-blocks-index-lengths: %d(num), %d(offset), %d(length)",
						vbih.blockNumByteLength, vbih.blockOffsetByteLength, vbih.blockLengthByteLength)
				}
				fmt.Fprintf(w, "%10d    %s block:%d/%d%s",
					b.Offset+uint64(enc.Offset), kv.K.UserKey,
					bh.Offset, bh.Length, vbihStr)
			})
			formatTrailer()
		case "value-block":
			// We don't peer into the value-block since it can't be interpreted
			// without the valueHandles.
		case "value-index":
			// We have already read the value-index to construct the list of
			// value-blocks, so no need to do it again.
		}

		h.Release()
	}

	last := blocks[len(blocks)-1]
	fmt.Fprintf(w, "%10d  EOF\n", last.Offset+last.Length)
}

func decodeVarint(ptr unsafe.Pointer) (uint32, unsafe.Pointer) {
	if a := *((*uint8)(ptr)); a < 128 {
		return uint32(a),
			unsafe.Pointer(uintptr(ptr) + 1)
	} else if a, b := a&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 1))); b < 128 {
		return uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 2)
	} else if b, c := b&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 2))); c < 128 {
		return uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 3)
	} else if c, d := c&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 3))); d < 128 {
		return uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 4)
	} else {
		d, e := d&0x7f, *((*uint8)(unsafe.Pointer(uintptr(ptr) + 4)))
		return uint32(e)<<28 | uint32(d)<<21 | uint32(c)<<14 | uint32(b)<<7 | uint32(a),
			unsafe.Pointer(uintptr(ptr) + 5)
	}
}
