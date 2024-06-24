// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/golang/snappy"
)

// The available compression types.
const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	ZstdCompression
	NCompression
)

// Compression is the per-block compression algorithm to use.
type Compression int

func (c Compression) String() string {
	switch c {
	case DefaultCompression:
		return "Default"
	case NoCompression:
		return "NoCompression"
	case SnappyCompression:
		return "Snappy"
	case ZstdCompression:
		return "ZSTD"
	default:
		return "Unknown"
	}
}

// CompressionFromString returns an sstable.Compression from its
// string representation. Inverse of c.String() above.
func CompressionFromString(s string) Compression {
	switch s {
	case "Default":
		return DefaultCompression
	case "NoCompression":
		return NoCompression
	case "Snappy":
		return SnappyCompression
	case "ZSTD":
		return ZstdCompression
	default:
		return DefaultCompression
	}
}

// Type is an enum serialized within the block format indicating the compression
// algorithm used to compress a block.
type Type byte

const (
	// NoCompressionBlockType is the block type of an uncompressed block.
	NoCompressionBlockType Type = 0

	// The block type gives the per-block compression format.
	// These constants are part of the file format and should not be changed.
	// They are different from the Compression constants because the latter
	// are designed so that the zero value of the Compression type means to
	// use the default compression (which is snappy).
	// Not all compression types listed here are supported.
	snappyCompressionBlockType Type = 1
	zlibCompressionBlockType   Type = 2
	bzip2CompressionBlockType  Type = 3
	lz4CompressionBlockType    Type = 4
	lz4hcCompressionBlockType  Type = 5
	xpressCompressionBlockType Type = 6
	zstdCompressionBlockType   Type = 7
)

// IsCompressed returns true if the block type indicates that the block is
// compressed.
func (t Type) IsCompressed() bool {
	return t != NoCompressionBlockType
}

// String implements fmt.Stringer.
func (t Type) String() string {
	switch t {
	case 0:
		return "none"
	case 1:
		return "snappy"
	case 2:
		return "zlib"
	case 3:
		return "bzip2"
	case 4:
		return "lz4"
	case 5:
		return "lz4hc"
	case 6:
		return "xpress"
	case 7:
		return "zstd"
	default:
		panic(errors.Newf("sstable: unknown block type: %d", t))
	}
}

const TrailerLen = 5

type Trailer = [TrailerLen]byte

type CompressionBuf struct {
	// compressedBuf is the destination buffer for compression. It is re-used over the
	// lifetime of the blockBuf, avoiding the allocation of a temporary buffer for each block.
	compressedBuf []byte
	checksummer   Checksummer
}

func (b *CompressionBuf) CompressAndChecksum(
	algo Compression, cksumAlgo ChecksumType, blk []byte,
) (compressed []byte, trailer Trailer) {
	// Compress the buffer, discarding the result if the improvement isn't at
	// least 12.5%.
	typ, compressed := compress(algo, blk, b.compressedBuf)
	if typ != NoCompressionBlockType && cap(compressed) > cap(b.compressedBuf) {
		b.compressedBuf = compressed[:cap(compressed)]
	}
	if len(compressed) < len(blk)-len(blk)/8 {
		blk = compressed
	} else {
		typ = NoCompressionBlockType
	}

	trailer[0] = byte(typ)

	// Calculate the checksum.
	b.checksummer.Type = cksumAlgo
	checksum := b.checksummer.Checksum(blk, trailer[:1])
	binary.LittleEndian.PutUint32(trailer[1:5], checksum)
	return blk, trailer
}

func decompressedLen(blockType Type, b []byte) (int, int, error) {
	switch blockType {
	case NoCompressionBlockType:
		return 0, 0, nil
	case snappyCompressionBlockType:
		l, err := snappy.DecodedLen(b)
		return l, 0, err
	case zstdCompressionBlockType:
		// This will also be used by zlib, bzip2 and lz4 to retrieve the decodedLen
		// if we implement these algorithms in the future.
		decodedLenU64, varIntLen := binary.Uvarint(b)
		if varIntLen <= 0 {
			return 0, 0, base.CorruptionErrorf("pebble/table: compression block has invalid length")
		}
		return int(decodedLenU64), varIntLen, nil
	default:
		return 0, 0, base.CorruptionErrorf("pebble/table: unknown block compression: %d", errors.Safe(blockType))
	}
}

// decompressInto decompresses compressed into buf. The buf slice must have the
// exact size as the decompressed value.
func decompressInto(blockType Type, compressed []byte, buf []byte) error {
	var result []byte
	var err error
	switch blockType {
	case snappyCompressionBlockType:
		result, err = snappy.Decode(buf, compressed)
	case zstdCompressionBlockType:
		result, err = decodeZstd(buf, compressed)
	default:
		return base.CorruptionErrorf("pebble/table: unknown block compression: %d", errors.Safe(blockType))
	}
	if err != nil {
		return base.MarkCorruptionError(err)
	}
	if len(result) != len(buf) || (len(result) > 0 && &result[0] != &buf[0]) {
		return base.CorruptionErrorf("pebble/table: decompressed into unexpected buffer: %p != %p",
			errors.Safe(result), errors.Safe(buf))
	}
	return nil
}

// decompressBlock decompresses an SST block, with manually-allocated space.
// NB: If decompressBlock returns (nil, nil), no decompression was necessary and
// the caller may use `b` directly.
func decompressBlock(blockType Type, b []byte) (*cache.Value, error) {
	if blockType == NoCompressionBlockType {
		return nil, nil
	}
	// first obtain the decoded length.
	decodedLen, prefixLen, err := decompressedLen(blockType, b)
	if err != nil {
		return nil, err
	}
	b = b[prefixLen:]
	// Allocate sufficient space from the cache.
	decoded := cache.Alloc(decodedLen)
	decodedBuf := decoded.Buf()
	if err := decompressInto(blockType, b, decodedBuf); err != nil {
		cache.Free(decoded)
		return nil, err
	}
	return decoded, nil
}

func CompressAndChecksum(
	algo Compression, checksum Checksummer, src []byte, dst *[]byte,
) (typ Type, n int, b []byte) {
	typ = NoCompressionBlockType
	if algo != NoCompression {
		typ, *dst = compress(algo, src, (*dst)[:cap(*dst)])
		if len(*dst) < len(src)-len(src)/8 {
			b = *dst
		} else {
			b = src
			typ = NoCompressionBlockType
		}
	}
	n = len(b)
	if n+TrailerLen > cap(b) {
		block := make([]byte, n+TrailerLen)
		copy(block, b)
		b = block
	} else {
		b = b[:n+TrailerLen]
	}
	b[n] = byte(typ)
	checksum.Checksum(b, b[n:n+1])
	binary.LittleEndian.PutUint32(b[n+1:], checksum.Checksum(b, b[n:n+1]))
	return typ, n, b
}

// compress compresses an sstable block, using compressedBuf as the desired
// destination. Most callers should prefer to use
// CompressionBuf.CompressAndChecksum.
func compress(
	compression Compression, b []byte, compressedBuf []byte,
) (blockType Type, compressed []byte) {
	switch compression {
	case SnappyCompression:
		return snappyCompressionBlockType, snappy.Encode(compressedBuf, b)
	case NoCompression:
		return NoCompressionBlockType, b
	}

	if len(compressedBuf) < binary.MaxVarintLen64 {
		compressedBuf = append(compressedBuf, make([]byte, binary.MaxVarintLen64-len(compressedBuf))...)
	}
	varIntLen := binary.PutUvarint(compressedBuf, uint64(len(b)))
	switch compression {
	case ZstdCompression:
		return zstdCompressionBlockType, encodeZstd(compressedBuf, varIntLen, b)
	default:
		return NoCompressionBlockType, b
	}
}
