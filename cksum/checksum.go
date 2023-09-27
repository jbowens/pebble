package cksum

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/zeebo/xxh3"
)

// This file contains the checksum data type and the primitive functions for
// computing checksums over data.
//
// For now this uses xxh3 for prototyping expediency, but 8 bytes seems
// excessive.
//
// TODO considerations:
// - Block properties
// - Value prefix byte
//
// All data must either be:
// - end-to-end checksummed
// - verified during read-back

// Size is the length in bytes of checksums produced by this package.
const Size = 8

var hashSeed = xxh3.HashSeed
var hashStringSeed = xxh3.HashStringSeed

// Each data type has its own seed to differentiate data types.

const (
	seedUnversionedKey uint64 = 0x01
	seedVersion        uint64 = 0x02
	seedValue          uint64 = 0x03
	seedTrailer        uint64 = 0x04
	seedSpanKey        uint64 = 0x05
)

// Checksum represents a computed checksum.
type Checksum [Size]byte

// XOR performs a bitwise XOR with the provided checksum, returning the result.
func (c Checksum) XOR(o Checksum) (out Checksum) {
	for i := 0; i < Size; i++ {
		out[i] = c[i] ^ o[i]
	}
	return out
}

// ChecksumKey computes a checksum over the provided key. The computation to
// checksum a key depends on whether the key is an unversioned prefix key, or a
// key with a version suffix. ChecksumKey requires the caller provide a Split
// function in order to make this determination.
//
// If the caller already knows whether the key is versioned, they should use
// ChecksumUnversionedKey or ChecksumVersionedKey accordingly.
func ChecksumKey(k []byte, split base.Split) Checksum {
	// When a key is suffixed, its checksum is computed over the unversioned
	// prefix and XORed with the checksum over the version suffix.
	i := len(k)
	if split != nil {
		i = split(k)
	}
	v := hashSeed(k[:i], seedUnversionedKey)
	if i < len(k) {
		// This key is composed of an unversioned prefix and a version suffix.
		// Compute its checksum as:
		//     hash(<prefix>, seedUnversionedKey)
		//                 XOR
		//     hash(<suffix>, seedVersion)
		v ^= hashSeed(k[i:], seedVersion)
	}
	return uint64AsChecksum(v)
}

// ChecksumUnversionedKey computes a checksum over a key that is known to be an
// unversioned prefix key for which Split(k) == len(k).
func ChecksumUnversionedKey(k []byte) Checksum {
	return computeChecksum(k, seedUnversionedKey)
}

// ChecksumVersion computes a checksum over a version or "suffix."
func ChecksumVersion(v []byte) Checksum {
	return computeChecksum(v, seedVersion)
}

// ChecksumVersionedKey computes a checksum over a key that is known to be a
// versioned key, with the version suffix beginning at byte i.
func ChecksumVersionedKey(k []byte, i int) Checksum {
	return uint64AsChecksum(hashSeed(k[:i], seedUnversionedKey) ^ hashSeed(k[i:], seedVersion))
}

// ChecksumValue computes a checksum over a value.
func ChecksumValue(v []byte) Checksum {
	return computeChecksum(v, seedValue)
}

// ChecksumTrailer computes a checksum over an internal key trailer.
func ChecksumTrailer(t uint64) Checksum {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], t)
	return computeChecksum(b[:], seedTrailer)
}

// ChecksumSpanKey computes a per-span key checksum.
func ChecksumSpanKey(trailer uint64, suffix, value []byte) Checksum {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], trailer)
	return uint64AsChecksum(
		hashSeed(b[:], seedTrailer) ^ hashSeed(suffix, seedVersion) ^ hashSeed(value, seedValue),
	)
}

func computeChecksum(b []byte, seed uint64) (sum Checksum) {
	return uint64AsChecksum(hashSeed(b, seed))
}

func uint64AsChecksum(x uint64) (sum Checksum) {
	binary.LittleEndian.PutUint64(sum[:], x)
	return sum
}
