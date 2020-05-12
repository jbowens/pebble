// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

// Cleaner exports the base.Cleaner type.
type Cleaner = base.Cleaner

// DeleteCleaner exports the base.DeleteCleaner type.
type DeleteCleaner = base.DeleteCleaner

// ArchiveCleaner exports the base.ArchiveCleaner type.
type ArchiveCleaner = base.ArchiveCleaner

// SelectiveArchiveCleaner returns a Cleaner and an EventListener.
// If a Pebble instance is configured to use both, the cleaner will archive
// manifests and any sstables created through ingest or flush. Any sstables
// created by compactions, log files, etc will be deleted.
func SelectiveArchiveCleaner() (Cleaner, EventListener) {
	a := &selectiveArchiver{
		compactCreated: make(map[FileNum]bool),
	}
	return a, a.eventListener()
}

type selectiveArchiver struct {
	mu             sync.Mutex
	compactCreated map[FileNum]bool
	archive        ArchiveCleaner
	delete         DeleteCleaner
}

func (a *selectiveArchiver) String() string { return "selective" }

func (a *selectiveArchiver) Clean(fs vfs.FS, fileType base.FileType, path string) error {
	if fileType == base.FileTypeManifest {
		return a.archive.Clean(fs, fileType, path)
	}
	if fileType != base.FileTypeTable {
		return a.delete.Clean(fs, fileType, path)
	}
	_, num, ok := base.ParseFilename(fs, fs.PathBase(path))
	if !ok {
		return errors.New("unable to parse filename")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.compactCreated[num] {
		delete(a.compactCreated, num)
		return a.delete.Clean(fs, fileType, path)
	}
	return a.archive.Clean(fs, fileType, path)
}

func (a *selectiveArchiver) eventListener() EventListener {
	el := EventListener{
		CompactionEnd: func(info CompactionInfo) {
			a.mu.Lock()
			defer a.mu.Unlock()

			for _, outputTable := range info.Output.Tables {
				// A table in the output of a compaction might also be in the input of
				// a compaction if it was moved.
				var fromInput bool
				for _, inputTables := range info.Input.Tables {
					for _, inputTable := range inputTables {
						fromInput = fromInput || inputTable.FileNum == outputTable.FileNum
					}
				}
				if !fromInput {
					a.compactCreated[outputTable.FileNum] = true
				}
			}
		},
	}
	return el
}
