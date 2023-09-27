package cksum

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// MakeKey takes a key—either versioned or unversioned—and returns a Key that
// holds the original key and its computed checksum. MakeKey only wraps the
// provided key bytes and does not copy.
func MakeKey(key []byte, split base.Split) Key {
	return Key{Key: key, Checksum: ChecksumKey(key, split)}
}

// Key holds a versioned or unversioned key and its corresponding checksum.
type Key struct {
	Key      []byte
	Checksum Checksum
}

// Verify recomputes the key's checksum and compares it against the checksum
// contained alongside the Key.
func (k Key) Verify(split base.Split) error {
	var recomputed uint64
	if i := split(k.Key); i < len(k.Key) {
		recomputed = hashSeed(k.Key[:i], seedUnversionedKey) ^ hashSeed(k.Key[i:], seedVersion)
	} else {
		recomputed = hashSeed(k.Key, seedUnversionedKey)
	}
	if recomputed != binary.LittleEndian.Uint64(k.Checksum[:]) {
		return errors.Errorf("key %q's checksum of %x does not match recomputed value %x",
			k.Key, k.Checksum[:], recomputed)
	}
	return nil
}

// MakeVersion computes a checksum over a version (also known as a "suffix"
// within Pebble). MakeVersion only wraps the version bytes and does not
// copy.
func MakeVersion(v []byte) Version {
	return Version{Version: v, Checksum: ChecksumVersion(v)}
}

// Version holds a version (or Pebble "suffix") and its checksum.
type Version struct {
	Version []byte
	Checksum
}

// MakeInternalKey takes a Key and a corresponding internal key Trailer and
// returns an InternalKey. ChecksumInternalKey only wraps the provided Key and
// does not copy its bytes.
func MakeInternalKey(k Key, trailer uint64) InternalKey {
	return InternalKey{
		InternalKey: base.InternalKey{UserKey: k.Key, Trailer: trailer},
		Checksum:    ChecksumTrailer(trailer).XOR(k.Checksum),
	}
}

// InternalKey holds a Pebble internal key along with its checksum.
type InternalKey struct {
	base.InternalKey
	Checksum
}

// WithNewTrailer returns a new InternalKey with the same user key but the
// provided trailer. The returned InternalKey will have a correct checksum if
// the original InternalKey had a correct checksum.
func (ik InternalKey) WithNewTrailer(t uint64) InternalKey {
	return InternalKey{
		InternalKey: base.InternalKey{
			UserKey: ik.InternalKey.UserKey,
			Trailer: t,
		},
		// Compute the new InternalKey's checksum by XOR-ing out the old trailer
		// and then XOR-ing in the new trailer.
		Checksum: ChecksumTrailer(ik.Trailer).
			XOR(ik.Checksum).
			XOR(ChecksumTrailer(t)),
	}
}

// MakeUnversionedKey computes a checksum of the provided unversioned key
// (also known as a "prefix" within Pebble), returning an UnversionedKey struct
// that holds the original key and its computed checksum. ChecksumUnversionedKey
// only wraps the provided key bytes and does not copy.
func MakeUnversionedKey(uk []byte) UnversionedKey {
	return UnversionedKey{Key: uk, Checksum: ChecksumUnversionedKey(uk)}
}

// An UnversionedKey holds an unversioned user key and its checksum.
//
// Invariant: Split(uk.Key()) == len(uk.Key()).
type UnversionedKey struct {
	Key []byte
	Checksum
}

// MakeVersionedKey computes a checksum of the provided versioned key (a key
// consisting of both an unversioned user key prefix and a version suffix),
// returning an UnversionedKey struct that holds the original key and its
// computed checksum. MakeVersionedKey only wraps the provided key bytes and
// does not copy.
//
// MakeVersionedKey provide an index i < len(key) that separates the key prefix
// and the version suffix.
func MakeVersionedKey(key []byte, i int) VersionedKey {
	return VersionedKey{Key: key, Checksum: ChecksumVersionedKey(key, i)}
}

// A VersionedKey is a versioned key, consisting of an unversioned user key
// prefix and a version suffix.
//
// Invariant: Split(uk.Key()) < len(uk.Key()).
type VersionedKey struct {
	Key []byte
	Checksum
}

// AsKey returns this versioned key as a Key, erasing the type system's
// knowledge that the underlying key is versioned.
func (vk VersionedKey) AsKey() Key { return Key{Key: vk.Key, Checksum: vk.Checksum} }

// Range deletions
//
// Internal key:
// <start user key> <trailer> <CHECKSUM(<start user key>) XOR CHECKSUM(<trailer>)>
// <end user key> <CHECKSUM(<end user key>)>

// Range keys
//
// Internal key:
// <start user key> <trailer> <CHECKSUM(<start>) XOR CHECKSUM(<trailer>)>
//
// Internal value:
// <end user key> <CHECKSUM(<end user key>)>
// [<suffix> <value> <CHECKSUM(<suffix>) XOR CHECKSUM(<value>)>]
// [<suffix> <value> <CHECKSUM(<suffix>) XOR CHECKSUM(<value>)>]

// Span represents a set of keys over a span of user key space. All of the keys
// within a Span are applied across the span's key span indicated by Start and
// End. Each internal key applied over the user key span appears as a separate
// Key, with its own kind and sequence number. Optionally, each Key may also
// have a Suffix and/or Value.
//
// Note that the start user key is inclusive and the end user key is exclusive.
//
// Currently the only supported key kinds are:
//
//	RANGEDEL, RANGEKEYSET, RANGEKEYUNSET, RANGEKEYDEL.
type Span struct {
	// Start and End hold the user key range of all the contained items, with an
	// inclusive start key and exclusive end key. Both Start and End must be
	// non-nil, or both nil if representing an invalid Span.
	//
	// Start and End each hold their own individual checksum.
	Start, End Key
	Keys       []SpanKey
}

// A SpanKey defines a key that's applied over a key span.
type SpanKey struct {
	Trailer uint64
	Suffix  []byte
	Value   []byte
	Checksum
}
