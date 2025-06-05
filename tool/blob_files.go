// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
)

// blobFileMappings contains state for retrieving a value separated into a blob
// file.
//
// An sstable alone does not contain sufficient information to read a value from
// a blob file. It requires the manifest to map the ReferenceIDs encoded within
// the sstable to physical blob files. The blobFileMappings type contains
// sufficient information to perform this mapping.
type blobFileMappings struct {
	references map[base.TableNum]*manifest.BlobReferences
	blobFiles  map[base.BlobFileID][]physicalBlobFile
	fetcher    blob.ValueFetcher
	provider   debugReaderProvider
	stderr     io.Writer
}

type physicalBlobFile struct {
	FileNum  base.DiskFileNum
	Metadata objstorage.ObjectMetadata
	Err      error
}

// LoadValueBlobContext returns a TableBlobContext that configures a sstable
// iterator to fetch the value stored in a blob file, using the mappings in this
// blobFileMappings.
func (m *blobFileMappings) LoadValueBlobContext(tableNum base.TableNum) sstable.TableBlobContext {
	return sstable.TableBlobContext{
		ValueFetcher: &m.fetcher,
		References:   m.references[tableNum],
	}
}

// LookupBlobFile returns the physical blob file number for the given blob file
// ID. It returns false for the second return value if no blob file is found.
func (m *blobFileMappings) LookupBlobFile(fileID base.BlobFileID) (base.DiskFileNum, bool) {
	for _, file := range m.blobFiles[fileID] {
		if file.Err == nil {
			return file.FileNum, true
		}
	}
	// For every unique physical blob file with this fileID that we found across
	// all manifests, print the file number and error.
	fmt.Fprintf(m.stderr, "no extant blob files found for fileID %s\n", fileID)
	for _, file := range m.blobFiles[fileID] {
		fmt.Fprintf(m.stderr, "  %s: %s\n", file.Metadata.DiskFileNum, file.Err)
	}
	return 0, false
}

// Close releases any resources held by the blobFileMappings.
func (m *blobFileMappings) Close() error {
	return errors.CombineErrors(m.fetcher.Close(), m.provider.objProvider.Close())
}

// newBlobFileMappings builds a blobFileMappings from a list of manifests.
//
// Possibly benign errors are printed to stderr. For example, if a process exits
// during a manifest rotation, we may encounter an error reading a manifest, but
// we'll still have sufficient information to read separated values from all
// tables in the current version of the LSM.
//
// The returned blobFileMappings must be closed to release resources.
func newBlobFileMappings(
	stderr io.Writer, fs vfs.FS, dir string, manifests []fileLoc,
) (*blobFileMappings, error) {
	settings := objstorageprovider.DefaultSettings(fs, dir)
	provider, err := objstorageprovider.Open(settings)
	if err != nil {
		return nil, err
	}
	mappings := blobFileMappings{
		references: make(map[base.TableNum]*manifest.BlobReferences),
		blobFiles:  make(map[base.BlobFileID][]physicalBlobFile),
		provider:   debugReaderProvider{objProvider: provider},
		stderr:     stderr,
	}
	mappings.fetcher.Init(mappings.LookupBlobFile, &mappings.provider, block.ReadEnv{})
	for _, fl := range manifests {
		err := func() error {
			mf, err := fs.Open(fl.path)
			if err != nil {
				return err
			}
			defer mf.Close()

			rr := record.NewReader(mf, fl.DiskFileNum)
			for {
				r, err := rr.Next()
				if err != nil {
					if err != io.EOF {
						return err
					}
					break
				}
				var ve manifest.VersionEdit
				if err = ve.Decode(r); err != nil {
					return err
				}
				for _, nf := range ve.NewTables {
					mappings.references[nf.Meta.TableNum] = &nf.Meta.BlobReferences
				}
				// Collect all the blob file metadatas. Some of these may
				// ultimately be replaced if a blob file is replaced. However,
				// in the context of the pebble tool, we're not trying to read
				// just the latest state. We collect the file numbers of all
				// physical blob files and check for their existence in the
				// objstorage. The LookupBlobFile method will use any of these
				// that exists in object storage, and print information about
				// the mapping used.
				for _, nf := range ve.NewBlobFiles {
					meta, err := mappings.provider.objProvider.Lookup(base.FileTypeBlob, nf.Physical.FileNum)
					mappings.blobFiles[nf.FileID] = append(mappings.blobFiles[nf.FileID], physicalBlobFile{
						FileNum:  nf.Physical.FileNum,
						Metadata: meta,
						Err:      err,
					})
				}
			}
			return nil
		}()
		if err != nil {
			fmt.Fprintf(stderr, "error reading manifest %s: %v\n", fl.path, err)
		}
	}
	return &mappings, nil
}
