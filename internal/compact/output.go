// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
)

// NewTableOutput constructs a new PendingOutput for a sstable.
func NewTableOutput(objMeta objstorage.ObjectMetadata, writer sstable.RawWriter) *PendingOutput {
	return &PendingOutput{
		CreationTime: time.Now(),
		TableObjMeta: objMeta,
		TableWriter:  writer,
	}
}

// An PendingOutput is an individual pending output used by a compaction.
type PendingOutput struct {
	CreationTime time.Time
	TableObjMeta objstorage.ObjectMetadata
	TableWriter  sstable.RawWriter
}

// EstimatedSize returns an estimate of the disk space corresponding to the
// output.
func (o *PendingOutput) EstimatedSize() uint64 {
	return o.TableWriter.EstimatedSize()
}

// Close closes the output's file(s) and returns the metadata for the finished
// table.
func (o *PendingOutput) Close(
	cmp base.Compare, compactionBounds base.UserKeyBounds, splitKey []byte,
) (Output, error) {
	output := Output{
		CreationTime: o.CreationTime,
		ObjMeta:      o.TableObjMeta,
	}
	err := o.TableWriter.Close()
	if err != nil {
		return output, err
	}
	meta, err := o.TableWriter.Metadata()
	if err != nil {
		return output, err
	}
	err = validateWriterMeta(cmp, meta, compactionBounds, splitKey)
	output.WriterMeta = *meta
	return output, err
}

// validateWriterMeta runs some sanity cehcks on the WriterMetadata on an output
// table that was just finished. splitKey is the key where the table must have
// ended (or nil).
func validateWriterMeta(
	cmp base.Compare,
	meta *sstable.WriterMetadata,
	compactionBounds base.UserKeyBounds,
	splitKey []byte,
) error {
	if !meta.HasPointKeys && !meta.HasRangeDelKeys && !meta.HasRangeKeys {
		return base.AssertionFailedf("output table has no keys")
	}

	var err error
	checkBounds := func(smallest, largest base.InternalKey, description string) {
		bounds := base.UserKeyBoundsFromInternal(smallest, largest)
		if !compactionBounds.ContainsBounds(cmp, &bounds) {
			err = errors.CombineErrors(err, base.AssertionFailedf(
				"output table %s bounds %s extend beyond compaction bounds %s",
				description, bounds, compactionBounds,
			))
		}
		if splitKey != nil && bounds.End.IsUpperBoundFor(cmp, splitKey) {
			err = errors.CombineErrors(err, base.AssertionFailedf(
				"output table %s bounds %s extend beyond split key %s",
				description, bounds, splitKey,
			))
		}
	}

	if meta.HasPointKeys {
		checkBounds(meta.SmallestPoint, meta.LargestPoint, "point key")
	}
	if meta.HasRangeDelKeys {
		checkBounds(meta.SmallestRangeDel, meta.LargestRangeDel, "range del")
	}
	if meta.HasRangeKeys {
		checkBounds(meta.SmallestRangeKey, meta.LargestRangeKey, "range key")
	}
	return err
}
