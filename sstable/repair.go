// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/internal/crc"
)

// Repair instructs the Reader to attempt to repair any corruption encountered
// during reading the sstable. If any repairs are applied, instructions for
// re-applying them are appended to the slice pointed to by instructions.
func Repair(instructions *[]RepairInstruction) ReaderOption {
	return repairOpt{instructions}
}

type repairOpt struct {
	dst *[]RepairInstruction
}

// readerApply implements ReaderOption.
func (ro repairOpt) readerApply(r *Reader) {
	r.repairInstructions = ro.dst
}

// RepairInstruction ...
type RepairInstruction struct {
	// Offset ...
	Offset uint64
	// Old ...
	Old byte
	// New ...
	New byte
	// Desc ...
	Desc string
}

func (ri RepairInstruction) String() string {
	return ri.Desc
}

func repairBlockChecksum(checksumType ChecksumType, b []byte, bh BlockHandle) (inst []RepairInstruction, ok bool) {
	off, rep, ok := withEveryByteReplacement(b, func(b []byte) bool {
		expectedChecksum := binary.LittleEndian.Uint32(b[bh.Length+1:])
		switch checksumType {
		case ChecksumTypeCRC32c:
			return expectedChecksum == crc.New(b[:bh.Length+1]).Value()
		case ChecksumTypeXXHash64:
			return expectedChecksum == uint32(xxhash.Sum64(b[:bh.Length+1]))
		default:
			return false
		}
	})
	if ok {
		return []RepairInstruction{{
			Offset: bh.Offset + uint64(off),
			Old:    b[off],
			New:    rep,
			Desc: fmt.Sprintf("block %s: replacing byte %8b with %8b at file offset %d produces a matching %s checksum",
				bh, b[off], rep, bh.Offset+uint64(off), checksumType),
		}}, true
	}
	return nil, false
}

func withEveryByteReplacement(b []byte, testFunc func([]byte) bool) (byteOffset int, replacement byte, ok bool) {
	for i := 0; i < len(b); i++ {
		originalByte := b[i]
		for j := 0; j < 256; j++ {
			b[i] = byte(j)
			if testFunc(b) {
				return i, byte(j), true
			}
		}
		b[i] = originalByte
	}
	return 0, 0, false
}
