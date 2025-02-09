// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valblk

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
)

// DecodeBlockHandleFromIndex decodes the block handle for the given block
// number from the provided index block.
func DecodeBlockHandleFromIndex(vbiBlock []byte, blockNum uint32, indexHandle IndexHandle) (block.Handle, error) {
	w := indexHandle.RowWidth()
	off := w * int(blockNum)
	if len(vbiBlock) < off+w {
		return block.Handle{}, base.CorruptionErrorf(
			"index entry out of bounds: offset %d length %d block length %d",
			off, w, len(vbiBlock))
	}
	b := vbiBlock[off : off+w]
	n := int(indexHandle.BlockNumByteLength)
	bn := littleEndianGet(b, n)
	if uint32(bn) != blockNum {
		return block.Handle{},
			errors.Errorf("expected block num %d but found %d", blockNum, bn)
	}
	b = b[n:]
	n = int(indexHandle.BlockOffsetByteLength)
	blockOffset := littleEndianGet(b, n)
	b = b[n:]
	n = int(indexHandle.BlockLengthByteLength)
	blockLen := littleEndianGet(b, n)
	return block.Handle{Offset: blockOffset, Length: blockLen}, nil
}
