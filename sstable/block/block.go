// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/crc"
)

func Load(
	checksumType ChecksumType, blk Value, transform LoadTransform, bufferPool *BufferPool,
) (loaded Value, err error) {
	// A serialized sstable block is wrappen in a light envelope. At the tail is
	// 5-byte trailer consisting of a 1-byte block type (compression type) and a
	// 4-byte checksum. Depending on the block type, the compression payload may
	// be prefixed with an integer indicating the length of the decompressed
	// payload so that block loading can allocate a buffer exactly the right
	// size.
	//
	//     +=====================================================+
	//     |				  Block contents                     |
	//     |                                                     |
	//     | [ decompressed length ]                             |
	//     |                                                     |
	//     |   .... compressed block data ...                    |
	//     |                                                     |
	//     +=============+=======================================+ <-- trailerIndex
	//     |                  Block trailer                      |
	//     |                                                     |
	//     | 1-byte Type |            4-byte checksum            |
	//     +=====================================================+
	//
	compressedWithTrailer := blk.Get()
	trailerIndex := len(compressedWithTrailer) - TrailerLen

	// Verify the checksum. The checksum is the last 4 bytes of the block.
	if err := checkChecksum(checksumType, compressedWithTrailer, trailerIndex); err != nil {
		return Value{}, err
	}

	// The block type (the first byte of the trailer) tells us how to decompress
	// the block.
	typ := Type(compressedWithTrailer[trailerIndex])

	// If the type indicates the block is not compressed, just return the
	// compressed cache value with the trailer removed through truncation.
	if typ == NoCompressionBlockType {
		blk.Truncate(trailerIndex)
		return blk, nil
	}

	defer blk.Release()
	// Decode the prefix that encodes the length of the decompressed value, and
	// allocate a buffer to exactly fit the decompressed block.
	decodedLen, prefixLen, err := decompressedLen(typ, compressedWithTrailer[:trailerIndex])
	if err != nil {
		return Value{}, err
	}
	loaded = Alloc(decodedLen, bufferPool)
	if err := decompressInto(typ, compressedWithTrailer[prefixLen:trailerIndex], loaded.Get()); err != nil {
		return Value{}, err
	}

	// Apply the LoadTransform if one is provided. Transforming blocks is very
	// rare, so the extra copy of the transformed data is not problematic.
	if transform != nil {
		tmpTransformed, err := transform(loaded.Get())
		if err != nil {
			loaded.Release()
			return Value{}, err
		}
		transformed := Alloc(decodedLen, bufferPool)
		copy(transformed.Get(), tmpTransformed)
		loaded.Release()
		loaded = transformed
	}

	return loaded, nil
}

func LoadIntoBuffer(
	checksumType ChecksumType, blk []byte, dst []byte,
) (loaded []byte, buf []byte, err error) {
	trailerIndex := len(blk) - TrailerLen
	if err := checkChecksum(checksumType, blk, trailerIndex); err != nil {
		return nil, buf, err
	}
	typ := Type(blk[trailerIndex])
	blk = blk[:trailerIndex]
	if typ == NoCompressionBlockType {
		return blk, buf, nil
	}
	decompressedLen, prefix, err := decompressedLen(typ, blk)
	if err != nil {
		return nil, buf, err
	}
	buf = dst
	if cap(buf) < decompressedLen {
		buf = make([]byte, decompressedLen)
	}
	loaded = buf[:decompressedLen]
	err = decompressInto(typ, blk[prefix:], loaded)
	return loaded, buf, err

}

func checkChecksum(
	checksumType ChecksumType, compressedWithTrailer []byte, trailerIndex int,
) error {
	expectedChecksum := binary.LittleEndian.Uint32(compressedWithTrailer[trailerIndex+1:])
	var recomputedChecksum uint32
	switch checksumType {
	case ChecksumTypeCRC32c:
		recomputedChecksum = crc.New(compressedWithTrailer[:trailerIndex+1]).Value()
	case ChecksumTypeXXHash64:
		recomputedChecksum = uint32(xxhash.Sum64(compressedWithTrailer[:trailerIndex+1]))
	default:
		return errors.Errorf("unsupported checksum type: %d", checksumType)
	}
	if expectedChecksum != recomputedChecksum {
		return base.CorruptionErrorf(
			"checksum mismatch; %x and %x", expectedChecksum, recomputedChecksum)
	}
	return nil
}

// ChecksumType specifies the checksum used for blocks.
type ChecksumType byte

// The available checksum types.
const (
	ChecksumTypeNone     ChecksumType = 0
	ChecksumTypeCRC32c   ChecksumType = 1
	ChecksumTypeXXHash   ChecksumType = 2
	ChecksumTypeXXHash64 ChecksumType = 3
)

// String implements fmt.Stringer.
func (t ChecksumType) String() string {
	switch t {
	case ChecksumTypeCRC32c:
		return "crc32c"
	case ChecksumTypeNone:
		return "none"
	case ChecksumTypeXXHash:
		return "xxhash"
	case ChecksumTypeXXHash64:
		return "xxhash64"
	default:
		panic(errors.Newf("sstable: unknown checksum type: %d", t))
	}
}

// A Checksummer calculates checksums for blocks.
type Checksummer struct {
	Type     ChecksumType
	xxHasher *xxhash.Digest
}

// Checksum computes a checksum over the provided block and block type.
func (c *Checksummer) Checksum(block []byte, blockType []byte) (checksum uint32) {
	// Calculate the checksum.
	switch c.Type {
	case ChecksumTypeCRC32c:
		checksum = crc.New(block).Update(blockType).Value()
	case ChecksumTypeXXHash64:
		if c.xxHasher == nil {
			c.xxHasher = xxhash.New()
		} else {
			c.xxHasher.Reset()
		}
		c.xxHasher.Write(block)
		c.xxHasher.Write(blockType)
		checksum = uint32(c.xxHasher.Sum64())
	default:
		panic(errors.Newf("unsupported checksum type: %d", c.Type))
	}
	return checksum
}

// LoadTransform allows transformation of a block's contents when the block is
// loaded into the block cache.
type LoadTransform func([]byte) ([]byte, error)

// IterTransforms allow on-the-fly transformation of data at iteration time.
//
// These transformations could in principle be implemented as block transforms
// (at least for non-virtual sstables), but applying them during iteration is
// preferable.
type IterTransforms struct {
	SyntheticSeqNum    SyntheticSeqNum
	HideObsoletePoints bool
	SyntheticPrefix    SyntheticPrefix
	SyntheticSuffix    SyntheticSuffix
}

// NoTransforms is the default value for IterTransforms.
var NoTransforms = IterTransforms{}

// SyntheticSeqNum is used to override all sequence numbers in a table. It is
// set to a non-zero value when the table was created externally and ingested
// whole.
type SyntheticSeqNum base.SeqNum

// NoSyntheticSeqNum is the default zero value for SyntheticSeqNum, which
// disables overriding the sequence number.
const NoSyntheticSeqNum SyntheticSeqNum = 0

// SyntheticSuffix will replace every suffix of every key surfaced during block
// iteration. A synthetic suffix can be used if:
//  1. no two keys in the sst share the same prefix; and
//  2. pebble.Compare(prefix + replacementSuffix, prefix + originalSuffix) < 0,
//     for all keys in the backing sst which have a suffix (i.e. originalSuffix
//     is not empty).
type SyntheticSuffix []byte

// IsSet returns true if the synthetic suffix is not enpty.
func (ss SyntheticSuffix) IsSet() bool {
	return len(ss) > 0
}

// SyntheticPrefix represents a byte slice that is implicitly prepended to every
// key in a file being read or accessed by a reader.  Note that the table is
// assumed to contain "prefix-less" keys that become full keys when prepended
// with the synthetic prefix. The table's bloom filters are constructed only on
// the "prefix-less" keys in the table, but interactions with the file including
// seeks and reads, will all behave as if the file had been constructed from
// keys that did include the prefix. Note that all Compare operations may act on
// a prefix-less key as the synthetic prefix will never modify key metadata
// stored in the key suffix.
//
// NB: Since this transformation currently only applies to point keys, a block
// with range keys cannot be iterated over with a synthetic prefix.
type SyntheticPrefix []byte

// IsSet returns true if the synthetic prefix is not enpty.
func (sp SyntheticPrefix) IsSet() bool {
	return len(sp) > 0
}

// Apply prepends the synthetic prefix to a key.
func (sp SyntheticPrefix) Apply(key []byte) []byte {
	res := make([]byte, 0, len(sp)+len(key))
	res = append(res, sp...)
	res = append(res, key...)
	return res
}

// Invert removes the synthetic prefix from a key.
func (sp SyntheticPrefix) Invert(key []byte) []byte {
	res, ok := bytes.CutPrefix(key, sp)
	if !ok {
		panic(fmt.Sprintf("unexpected prefix: %s", key))
	}
	return res
}
