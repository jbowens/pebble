// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/valblk"
)

// defaultInternalValueConstructor is the default implementation of the
// block.GetInternalValueForPrefixAndValueHandler interface.
type defaultInternalValueConstructor struct {
	// blobValueFetcher is used as the base.ValueFetcher for values returned
	// that reference external blob files.
	blobValueFetcher base.ValueFetcher
	vbReader         valblk.Reader
	blobReferences   BlobReferences

	// lazyFetcher is the LazyFetcher value embedded in any LazyValue that we
	// return. It is used to avoid having a separate allocation for that.
	lazyFetcher base.LazyFetcher
}

// Assert that defaultInternalValueConstructor implements the
// block.GetInternalValueForPrefixAndValueHandler interface.
var _ block.GetInternalValueForPrefixAndValueHandler = (*defaultInternalValueConstructor)(nil)

// GetInternalValueForPrefixAndValueHandle returns a InternalValue for the
// given value prefix and value.
//
// The result is only valid until the next call to
// GetInternalValueForPrefixAndValueHandle. Use InternalValue.Clone if the
// lifetime of the InternalValue needs to be extended. For more details, see
// the "memory management" comment where LazyValue is declared.
func (i *defaultInternalValueConstructor) GetInternalValueForPrefixAndValueHandle(
	handle []byte,
) base.InternalValue {
	vp := block.ValuePrefix(handle[0])
	if vp.IsValueBlockHandle() {
		return i.vbReader.GetInternalValueForPrefixAndValueHandle(handle)
	} else if !vp.IsBlobValueHandle() {
		panic(errors.AssertionFailedf("block: %x is neither a valblk or blob handle prefix", vp))
	}

	// The first byte of [handle] is the valuePrefix byte.
	//
	// After that, is the inline-handle preface encoding a) the length of the
	// value and b) the blob reference index. We need to map the blob reference
	// index into a file number,
	//
	// The remainder of the handle (the suffix) encodes the value's location
	// within the blob file. We defer parsing of it until the user retrieves the
	// value. We propagate it as LazyValue.ValueOrHandle.
	preface, remainder := blob.DecodeInlineHandlePreface(handle[1:])

	i.lazyFetcher = base.LazyFetcher{
		Fetcher: i.blobValueFetcher,
		Attribute: base.AttributeAndLen{
			ValueLen:       preface.ValueLen,
			ShortAttribute: vp.ShortAttribute(),
		},
		BlobFileNum: i.blobReferences.FileNumByIndex(int(preface.ReferenceIndex)),
	}
	return base.MakeLazyValue(base.LazyValue{
		ValueOrHandle: remainder,
		Fetcher:       &i.lazyFetcher,
	})
}
