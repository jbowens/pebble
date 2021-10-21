// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan // import "github.com/cockroachdb/pebble/internal/keyspan"

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// Span is a span of key space. A span covers all of the keys in the
// range [start,end). Note that the start key is inclusive and the end
// key is exclusive.
type Span struct {
	Start base.InternalKey
	End   []byte
}

// Overlaps returns 0 if this span overlaps the other, -1 if there's no
// overlap and this span comes before the other, 1 if no overlap and this
// span comes after other.
func (s Span) Overlaps(cmp base.Compare, other Span) int {
	if cmp(s.Start.UserKey, other.Start.UserKey) == 0 && bytes.Equal(s.End, other.End) {
		if other.Start.SeqNum() < s.Start.SeqNum() {
			return -1
		}
		return 1
	}
	if cmp(s.End, other.Start.UserKey) <= 0 {
		return -1
	}
	if cmp(other.End, s.Start.UserKey) <= 0 {
		return 1
	}
	return 0
}

// Empty returns true if the span does not cover any keys.
func (s Span) Empty() bool {
	// TODO: not this.
	return s.Start.Kind() != base.InternalKeyKindRangeDelete
}

// Contains returns true if the specified key resides within the range
// span bounds.
func (s Span) Contains(cmp base.Compare, key []byte) bool {
	return cmp(s.Start.UserKey, key) <= 0 && cmp(key, s.End) < 0
}

// Covers returns true if the span covers keys at seqNum.
func (s Span) Covers(seqNum uint64) bool {
	return !s.Empty() && s.Start.SeqNum() > seqNum
}

func (s Span) String() string {
	if s.Empty() {
		return "<empty>"
	}
	return fmt.Sprintf("%s-%s#%d", s.Start.UserKey, s.End, s.Start.SeqNum())
}

// Pretty returns a formatter for the span.
func (s Span) Pretty(f base.FormatKey) fmt.Formatter {
	return prettySpan{s, f}
}

type prettySpan struct {
	Span
	formatKey base.FormatKey
}

func (s prettySpan) Format(fmts fmt.State, c rune) {
	if s.Empty() {
		fmt.Fprintf(fmts, "<empty>")
	}
	fmt.Fprintf(fmts, "%s-%s#%d", s.formatKey(s.Start.UserKey), s.formatKey(s.End), s.Start.SeqNum())
}
