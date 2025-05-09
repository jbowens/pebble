// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/manual"
	"github.com/cockroachdb/pebble/internal/whiteboard"
	"github.com/cockroachdb/pebble/internal/whiteboard/table"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/sharedcache"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/wal"
	"github.com/cockroachdb/redact"
	"github.com/prometheus/client_golang/prometheus"
)

// CacheMetrics holds metrics for the block and file cache.
type CacheMetrics = cache.Metrics

// FilterMetrics holds metrics for the filter policy
type FilterMetrics = sstable.FilterMetrics

// ThroughputMetric is a cumulative throughput metric. See the detailed
// comment in base.
type ThroughputMetric = base.ThroughputMetric

// SecondaryCacheMetrics holds metrics for the persistent secondary cache
// that caches commonly accessed blocks from blob storage on a local
// file system.
type SecondaryCacheMetrics = sharedcache.Metrics

// LevelMetrics holds per-level metrics such as the number of files and total
// size of the files, and compaction related metrics.
type LevelMetrics struct {
	// The number of sublevels within the level. The sublevel count corresponds
	// to the read amplification for the level. An empty level will have a
	// sublevel count of 0, implying no read amplification. Only L0 will have
	// a sublevel count other than 0 or 1.
	Sublevels int32
	// The total count of sstables in the level.
	TablesCount int64
	// The total size in bytes of the sstables in the level. Note that if tables
	// contain references to blob files, this quantity does not include the the
	// size of the blob files or the referenced values.
	TablesSize int64
	// The total number of virtual sstables in the level.
	VirtualTablesCount uint64
	// The total size of the virtual sstables in the level.
	VirtualTablesSize uint64
	// The estimated total physical size of all blob references across all
	// sstables in the level. The physical size is estimated based on the size
	// of referenced values and the values' blob file's compression ratios.
	EstimatedReferencesSize uint64
	// The level's compaction score, used to rank levels (0 if the level doesn't
	// need compaction). See candidateLevelInfo.
	Score float64
	// The level's fill factor (the ratio between the size of the level and the
	// ideal size). See candidateLevelInfo.
	FillFactor float64
	// The level's compensated fill factor. See candidateLevelInfo.
	CompensatedFillFactor float64
	// The number of incoming bytes from other levels' sstables read during
	// compactions. This excludes bytes moved and bytes ingested. For L0 this is
	// the bytes written to the WAL.
	TableBytesIn uint64
	// The number of sstable bytes ingested. The sibling metric for tables is
	// TablesIngested.
	TableBytesIngested uint64
	// The number of sstable bytes moved into the level by a "move" compaction.
	// The sibling metric for tables is TablesMoved.
	TableBytesMoved uint64
	// The number of bytes read for compactions at the level. This includes bytes
	// read from other levels (BytesIn), as well as bytes read for the level.
	TableBytesRead uint64
	// The number of bytes written to sstables during compactions. The sibling
	// metric for tables is TablesCompacted. This metric may be summed with
	// BytesFlushed to compute the total bytes written for the level.
	TableBytesCompacted uint64
	// The number of bytes written to sstables during flushes. The sibling
	// metrics for tables is TablesFlushed. This metric is always zero for all
	// levels other than L0.
	TableBytesFlushed uint64
	// The number of sstables compacted to this level.
	TablesCompacted uint64
	// The number of sstables flushed to this level.
	TablesFlushed uint64
	// The number of sstables ingested into the level.
	TablesIngested uint64
	// The number of sstables moved to this level by a "move" compaction.
	TablesMoved uint64
	// The number of sstables deleted in a level by a delete-only compaction.
	TablesDeleted uint64
	// The number of sstables excised in a level by a delete-only compaction.
	TablesExcised uint64
	// BlobBytesReadEstimate is an estimate of the physical bytes corresponding
	// to values referenced by sstables that were inputs into compactions
	// outputting into this level.
	BlobBytesReadEstimate uint64
	// BlobBytesWritten is the number of bytes written to blob files while
	// compacting sstables in this level.
	BlobBytesWritten uint64
	// BlobBytesFlushed is the number of bytes written to blob files while
	// flushing sstables. This metric is always zero for all levels other than
	// L0.
	BlobBytesFlushed uint64

	MultiLevel struct {
		// TableBytesInTop are the total bytes in a multilevel compaction coming
		// from the top level.
		TableBytesInTop uint64

		// TableBytesIn, exclusively for multiLevel compactions.
		TableBytesIn uint64

		// TableBytesRead, exclusively for multilevel compactions.
		TableBytesRead uint64
	}

	// Additional contains misc additional metrics that are not always printed.
	Additional struct {
		// The sum of Properties.ValueBlocksSize for all the sstables in this
		// level. Printed by LevelMetrics.format iff there is at least one level
		// with a non-zero value.
		ValueBlocksSize uint64
		// Cumulative metrics about bytes written to data blocks and value blocks,
		// via compactions (except move compactions) or flushes. Not printed by
		// LevelMetrics.format, but are available to sophisticated clients.
		BytesWrittenDataBlocks  uint64
		BytesWrittenValueBlocks uint64
	}
}

// AggregateSize returns an estimated physical size of the level's sstables and
// their referenced values stored in blob files. The size of physical sstables
// is exactly known. Virtual sstables' sizes are estimated, and the size of
// values stored in blob files is estimated based on the volume of referenced
// data and the blob file's compression ratio.
func (m *LevelMetrics) AggregateSize() int64 {
	return m.TablesSize + int64(m.EstimatedReferencesSize)
}

// Add updates the counter metrics for the level.
func (m *LevelMetrics) Add(u *LevelMetrics) {
	m.Sublevels += u.Sublevels
	m.TablesCount += u.TablesCount
	m.TablesSize += u.TablesSize
	m.VirtualTablesCount += u.VirtualTablesCount
	m.VirtualTablesSize += u.VirtualTablesSize
	m.EstimatedReferencesSize += u.EstimatedReferencesSize
	m.TableBytesIn += u.TableBytesIn
	m.TableBytesIngested += u.TableBytesIngested
	m.TableBytesMoved += u.TableBytesMoved
	m.TableBytesRead += u.TableBytesRead
	m.TableBytesCompacted += u.TableBytesCompacted
	m.TableBytesFlushed += u.TableBytesFlushed
	m.TablesCompacted += u.TablesCompacted
	m.TablesFlushed += u.TablesFlushed
	m.TablesIngested += u.TablesIngested
	m.TablesMoved += u.TablesMoved
	m.BlobBytesWritten += u.BlobBytesWritten
	m.BlobBytesFlushed += u.BlobBytesFlushed
	m.BlobBytesReadEstimate += u.BlobBytesReadEstimate
	m.MultiLevel.TableBytesInTop += u.MultiLevel.TableBytesInTop
	m.MultiLevel.TableBytesRead += u.MultiLevel.TableBytesRead
	m.MultiLevel.TableBytesIn += u.MultiLevel.TableBytesIn
	m.Additional.BytesWrittenDataBlocks += u.Additional.BytesWrittenDataBlocks
	m.Additional.BytesWrittenValueBlocks += u.Additional.BytesWrittenValueBlocks
	m.Additional.ValueBlocksSize += u.Additional.ValueBlocksSize
}

// WriteAmp computes the write amplification for compactions at this
// level. Computed as:
//
//	TableBytesFlushed + TableBytesCompacted + BlobBytesFlushed
//	---------------------------------------------------------
//	            TableBytesIn + BlobBytesWritten
func (m *LevelMetrics) WriteAmp() float64 {
	if m.TableBytesIn == 0 {
		return 0
	}
	return float64(m.TableBytesFlushed+m.TableBytesCompacted+m.BlobBytesFlushed) / float64(m.TableBytesIn+m.BlobBytesWritten)
}

var categoryCompaction = block.RegisterCategory("pebble-compaction", block.NonLatencySensitiveQoSLevel)
var categoryIngest = block.RegisterCategory("pebble-ingest", block.LatencySensitiveQoSLevel)
var categoryGet = block.RegisterCategory("pebble-get", block.LatencySensitiveQoSLevel)

// Metrics holds metrics for various subsystems of the DB such as the Cache,
// Compactions, WAL, and per-Level metrics.
//
// TODO(peter): The testing of these metrics is relatively weak. There should
// be testing that performs various operations on a DB and verifies that the
// metrics reflect those operations.
type Metrics struct {
	BlockCache CacheMetrics

	Compact struct {
		// The total number of compactions, and per-compaction type counts.
		Count                 int64
		DefaultCount          int64
		DeleteOnlyCount       int64
		ElisionOnlyCount      int64
		CopyCount             int64
		MoveCount             int64
		ReadCount             int64
		TombstoneDensityCount int64
		RewriteCount          int64
		MultiLevelCount       int64
		CounterLevelCount     int64
		// An estimate of the number of bytes that need to be compacted for the LSM
		// to reach a stable state.
		EstimatedDebt uint64
		// Number of bytes present in sstables being written by in-progress
		// compactions. This value will be zero if there are no in-progress
		// compactions.
		InProgressBytes int64
		// Number of compactions that are in-progress.
		NumInProgress int64
		// Number of compactions that were cancelled.
		CancelledCount int64
		// CancelledBytes the number of bytes written by compactions that were
		// cancelled.
		CancelledBytes int64
		// Total number of compactions that hit an error.
		FailedCount int64
		// NumProblemSpans is the current (instantaneous) count of "problem spans"
		// which temporarily block compactions.
		NumProblemSpans int
		// MarkedFiles is a count of files that are marked for
		// compaction. Such files are compacted in a rewrite compaction
		// when no other compactions are picked.
		MarkedFiles int
		// Duration records the cumulative duration of all compactions since the
		// database was opened.
		Duration time.Duration
	}

	Ingest struct {
		// The total number of ingestions
		Count uint64
	}

	Flush struct {
		// The total number of flushes.
		Count           int64
		WriteThroughput ThroughputMetric
		// Number of flushes that are in-progress. In the current implementation
		// this will always be zero or one.
		NumInProgress int64
		// AsIngestCount is a monotonically increasing counter of flush operations
		// handling ingested tables.
		AsIngestCount uint64
		// AsIngestCount is a monotonically increasing counter of tables ingested as
		// flushables.
		AsIngestTableCount uint64
		// AsIngestBytes is a monotonically increasing counter of the bytes flushed
		// for flushables that originated as ingestion operations.
		AsIngestBytes uint64
	}

	Filter FilterMetrics

	Levels [numLevels]LevelMetrics

	MemTable struct {
		// The number of bytes allocated by memtables and large (flushable)
		// batches.
		Size uint64
		// The count of memtables.
		Count int64
		// The number of bytes present in zombie memtables which are no longer
		// referenced by the current DB state. An unbounded number of memtables
		// may be zombie if they're still in use by an iterator. One additional
		// memtable may be zombie if it's no longer in use and waiting to be
		// recycled.
		ZombieSize uint64
		// The count of zombie memtables.
		ZombieCount int64
	}

	Keys struct {
		// The approximate count of internal range key set keys in the database.
		RangeKeySetsCount uint64
		// The approximate count of internal tombstones (DEL, SINGLEDEL and
		// RANGEDEL key kinds) within the database.
		TombstoneCount uint64
		// A cumulative total number of missized DELSIZED keys encountered by
		// compactions since the database was opened.
		MissizedTombstonesCount uint64
	}

	Snapshots struct {
		// The number of currently open snapshots.
		Count int
		// The sequence number of the earliest, currently open snapshot.
		EarliestSeqNum base.SeqNum
		// A running tally of keys written to sstables during flushes or
		// compactions that would've been elided if it weren't for open
		// snapshots.
		PinnedKeys uint64
		// A running cumulative sum of the size of keys and values written to
		// sstables during flushes or compactions that would've been elided if
		// it weren't for open snapshots.
		PinnedSize uint64
	}

	Table struct {
		// The number of bytes present in obsolete tables which are no longer
		// referenced by the current DB state or any open iterators.
		ObsoleteSize uint64
		// The count of obsolete tables.
		ObsoleteCount int64
		// The number of bytes present in zombie tables which are no longer
		// referenced by the current DB state but are still in use by an iterator.
		ZombieSize uint64
		// The count of zombie tables.
		ZombieCount int64
		// The count of sstables backing virtual tables.
		BackingTableCount uint64
		// The sum of the sizes of the BackingTableCount sstables that are backing virtual tables.
		BackingTableSize uint64
		// The number of sstables that are compressed with an unknown compression
		// algorithm.
		CompressedCountUnknown int64
		// The number of sstables that are compressed with the default compression
		// algorithm, snappy.
		CompressedCountSnappy int64
		// The number of sstables that are compressed with zstd.
		CompressedCountZstd int64
		// The number of sstables that are compressed with minlz.
		CompressedCountMinLZ int64
		// The number of sstables that are uncompressed.
		CompressedCountNone int64

		// Local file sizes.
		Local struct {
			// LiveSize is the number of bytes in live tables.
			LiveSize uint64
			// LiveCount is the number of live tables.
			LiveCount uint64
			// ObsoleteSize is the number of bytes in obsolete tables.
			ObsoleteSize uint64
			// ObsoleteCount is the number of obsolete tables.
			ObsoleteCount uint64
			// ZombieSize is the number of bytes in zombie tables.
			ZombieSize uint64
			// ZombieCount is the number of zombie tables.
			ZombieCount uint64
		}

		// Garbage bytes.
		Garbage struct {
			// PointDeletionsBytesEstimate is the estimated file bytes that will be
			// saved by compacting all point deletions. This is dependent on table
			// stats collection, so can be very incomplete until
			// InitialStatsCollectionComplete becomes true.
			PointDeletionsBytesEstimate uint64
			// RangeDeletionsBytesEstimate is the estimated file bytes that will be
			// saved by compacting all range deletions. This is dependent on table
			// stats collection, so can be very incomplete until
			// InitialStatsCollectionComplete becomes true.
			RangeDeletionsBytesEstimate uint64
		}

		// Whether the initial stats collection (for existing tables on Open) is
		// complete.
		InitialStatsCollectionComplete bool
		// The count of recently created sstables that need stats collection. This
		// does not include sstables that existed when the DB was opened, so the
		// value is only useful when InitialStatsCollectionComplete is true.
		PendingStatsCollectionCount int64
	}

	BlobFiles struct {
		// The count of all live blob files.
		LiveCount uint64
		// The physical file size of all live blob files.
		LiveSize uint64
		// ValueSize is the sum of the length of the uncompressed values in all
		// live (referenced by some sstable(s) within the current version) blob
		// files. ValueSize may be greater than LiveSize when compression is
		// effective. ValueSize includes bytes in live blob files that are not
		// actually reachable by any sstable key. If any value within the blob
		// file is reachable by a key in a live sstable, then the entirety of
		// the blob file's values are included within ValueSize.
		ValueSize uint64
		// ReferencedValueSize is the sum of the length of the uncompressed
		// values (in all live blob files) that are still referenced by keys
		// within live tables. Over the lifetime of a blob file, a blob file's
		// references are removed as some compactions choose to write new blob
		// files containing the same values or keys referencing the file's
		// values are deleted. ReferencedValueSize accounts the volume of bytes
		// that are actually reachable by some key in a live table.
		//
		// The difference between ValueSize and ReferencedValueSize is
		// (uncompressed) space amplification that could be reclaimed if all
		// blob files were rewritten, discarding values that are no longer
		// referenced by any keys in any sstables within the current version.
		ReferencedValueSize uint64
		// The count of all obsolete blob files.
		ObsoleteCount uint64
		// The physical size of all obsolete blob files.
		ObsoleteSize uint64
		// The count of all zombie blob files.
		ZombieCount uint64
		// The physical size of all zombie blob files.
		ZombieSize uint64
		// Local file sizes.
		Local struct {
			// LiveSize is the physical size of local live blob files.
			LiveSize uint64
			// LiveCount is the number of local live blob files.
			LiveCount uint64
			// ObsoleteSize is the physical size of local obsolete blob files.
			ObsoleteSize uint64
			// ObsoleteCount is the number of local obsolete blob files.
			ObsoleteCount uint64
			// ZombieSize is the physical size of local zombie blob files.
			ZombieSize uint64
			// ZombieCount is the number of local zombie blob files.
			ZombieCount uint64
		}
	}

	FileCache CacheMetrics

	// Count of the number of open sstable iterators.
	TableIters int64
	// Uptime is the total time since this DB was opened.
	Uptime time.Duration

	WAL struct {
		// Number of live WAL files.
		Files int64
		// Number of obsolete WAL files.
		ObsoleteFiles int64
		// Physical size of the obsolete WAL files.
		ObsoletePhysicalSize uint64
		// Size of the live data in the WAL files. Note that with WAL file
		// recycling this is less than the actual on-disk size of the WAL files.
		Size uint64
		// Physical size of the WAL files on-disk. With WAL file recycling,
		// this is greater than the live data in WAL files.
		//
		// TODO(sumeer): it seems this does not include ObsoletePhysicalSize.
		// Should the comment be updated?
		PhysicalSize uint64
		// Number of logical bytes written to the WAL.
		BytesIn uint64
		// Number of bytes written to the WAL.
		BytesWritten uint64
		// Failover contains failover stats. Empty if failover is not enabled.
		Failover wal.FailoverStats
	}

	LogWriter struct {
		FsyncLatency prometheus.Histogram
		record.LogWriterMetrics
	}

	CategoryStats []block.CategoryStatsAggregate

	SecondaryCacheMetrics SecondaryCacheMetrics

	private struct {
		optionsFileSize  uint64
		manifestFileSize uint64
	}

	manualMemory manual.Metrics
}

var (
	// FsyncLatencyBuckets are prometheus histogram buckets suitable for a histogram
	// that records latencies for fsyncs.
	FsyncLatencyBuckets = append(
		prometheus.LinearBuckets(0.0, float64(time.Microsecond*100), 50),
		prometheus.ExponentialBucketsRange(float64(time.Millisecond*5), float64(10*time.Second), 50)...,
	)

	// SecondaryCacheIOBuckets exported to enable exporting from package pebble to
	// enable exporting metrics with below buckets in CRDB.
	SecondaryCacheIOBuckets = sharedcache.IOBuckets
	// SecondaryCacheChannelWriteBuckets exported to enable exporting from package
	// pebble to enable exporting metrics with below buckets in CRDB.
	SecondaryCacheChannelWriteBuckets = sharedcache.ChannelWriteBuckets
)

// DiskSpaceUsage returns the total disk space used by the database in bytes,
// including live and obsolete files. This only includes local files, i.e.,
// remote files (as known to objstorage.Provider) are not included.
func (m *Metrics) DiskSpaceUsage() uint64 {
	var usageBytes uint64
	usageBytes += m.WAL.PhysicalSize
	usageBytes += m.WAL.ObsoletePhysicalSize
	usageBytes += m.Table.Local.LiveSize
	usageBytes += m.Table.Local.ObsoleteSize
	usageBytes += m.Table.Local.ZombieSize
	usageBytes += m.BlobFiles.Local.LiveSize
	usageBytes += m.BlobFiles.Local.ObsoleteSize
	usageBytes += m.BlobFiles.Local.ZombieSize
	usageBytes += m.private.optionsFileSize
	usageBytes += m.private.manifestFileSize
	// TODO(sumeer): InProgressBytes does not distinguish between local and
	// remote files. This causes a small error. Fix.
	usageBytes += uint64(m.Compact.InProgressBytes)
	return usageBytes
}

// NumVirtual is the number of virtual sstables in the latest version
// summed over every level in the lsm.
func (m *Metrics) NumVirtual() uint64 {
	var n uint64
	for _, level := range m.Levels {
		n += level.VirtualTablesCount
	}
	return n
}

// VirtualSize is the sum of the sizes of the virtual sstables in the
// latest version. BackingTableSize - VirtualSize gives an estimate for
// the space amplification caused by not compacting virtual sstables.
func (m *Metrics) VirtualSize() uint64 {
	var size uint64
	for _, level := range m.Levels {
		size += level.VirtualTablesSize
	}
	return size
}

// ReadAmp returns the current read amplification of the database.
// It's computed as the number of sublevels in L0 + the number of non-empty
// levels below L0.
func (m *Metrics) ReadAmp() int {
	var ramp int32
	for _, l := range m.Levels {
		ramp += l.Sublevels
	}
	return int(ramp)
}

// Total returns the sum of the per-level metrics and WAL metrics.
func (m *Metrics) Total() LevelMetrics {
	var total LevelMetrics
	for level := 0; level < numLevels; level++ {
		l := &m.Levels[level]
		total.Add(l)
	}
	// Compute total bytes-in as the bytes written to the WAL + bytes ingested.
	total.TableBytesIn = m.WAL.BytesWritten + total.TableBytesIngested
	// Add the total bytes-in to the total bytes-flushed. This is to account for
	// the bytes written to the log and bytes written externally and then
	// ingested.
	total.TableBytesFlushed += total.TableBytesIn
	return total
}

// RemoteTablesTotal returns the total number of remote tables and their total
// size. Remote tables are computed as the difference between total tables
// (live + obsolete + zombie) and local tables.
func (m *Metrics) RemoteTablesTotal() (count uint64, size uint64) {
	var liveTables, liveTableBytes int64
	for level := 0; level < numLevels; level++ {
		liveTables += m.Levels[level].TablesCount
		liveTableBytes += m.Levels[level].TablesSize
	}
	totalCount := liveTables + m.Table.ObsoleteCount + m.Table.ZombieCount
	localCount := m.Table.Local.LiveCount + m.Table.Local.ObsoleteCount + m.Table.Local.ZombieCount
	remoteCount := uint64(totalCount) - localCount

	totalSize := uint64(liveTableBytes) + m.Table.ObsoleteSize + m.Table.ZombieSize
	localSize := m.Table.Local.LiveSize + m.Table.Local.ObsoleteSize + m.Table.Local.ZombieSize
	remoteSize := totalSize - localSize

	return remoteCount, remoteSize
}

// Assert that Metrics implements redact.SafeFormatter.
var _ redact.SafeFormatter = (*Metrics)(nil)

// SafeFormat implements redact.SafeFormatter.
func (m *Metrics) SafeFormat(w redact.SafePrinter, _ rune) {
	w.SafeString(redact.SafeString(m.String()))
}

var (
	levelMetricsTableTopHeader = `LSM   |                             | valsep |        |   ingested   |       |       written      |    amp`
	levelMetricsTable          = table.Define(
		table.AutoIncrement[*LevelMetrics]("level", 5, table.AlignRight),
		table.Div[*LevelMetrics](),
		table.CountInt64("tables", 6, table.AlignRight, func(m *LevelMetrics) int64 { return m.TablesCount }),
		table.Literal[*LevelMetrics](" "),
		table.BytesInt64("size", 5, table.AlignRight, func(m *LevelMetrics) int64 { return m.TablesSize }),
		table.Literal[*LevelMetrics](" "),
		table.Bytes("val-bl", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.Additional.ValueBlocksSize }),
		table.Literal[*LevelMetrics](" "),
		table.Count("vtables", 7, table.AlignRight, func(m *LevelMetrics) uint64 { return m.VirtualTablesCount }),
		table.Div[*LevelMetrics](),
		table.Bytes("refsz", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.EstimatedReferencesSize }),
		table.Div[*LevelMetrics](),
		table.Bytes("in", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesIn }),
		table.Div[*LevelMetrics](),
		table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesIngested }),
		table.Literal[*LevelMetrics](" "),
		table.Bytes("size", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesIngested }),
		table.Div[*LevelMetrics](),
		table.Bytes("read", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesRead }),
		table.Div[*LevelMetrics](),
		table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesFlushed + m.TablesCompacted }),
		table.Literal[*LevelMetrics](" "),
		table.Bytes("tblsz", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesFlushed + m.TableBytesCompacted }),
		table.Literal[*LevelMetrics](" "),
		table.Bytes("blbsz", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.BlobBytesFlushed + m.BlobBytesWritten }),
		table.Div[*LevelMetrics](),
		table.Int("r", 3, table.AlignRight, func(m *LevelMetrics) int { return int(m.Sublevels) }),
		table.Literal[*LevelMetrics](" "),
		table.Float("w", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.WriteAmp() }),
	)
	levelMetricsTableBottomDivider       = strings.Repeat("-", levelMetricsTable.CumulativeFieldWidth)
	levelCompactionMetricsTableTopHeader = `COMPACTIONS               |     moved    |     multilevel`
	compactionLevelMetricsTable          = table.Define(
		table.AutoIncrement[*LevelMetrics]("level", 5, table.AlignRight),
		table.Div[*LevelMetrics](),
		table.Float("score", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.Score }),
		table.Literal[*LevelMetrics](" "),
		table.Float("ff", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.FillFactor }),
		table.Literal[*LevelMetrics](" "),
		table.Float("cff", 5, table.AlignRight, func(m *LevelMetrics) float64 { return m.CompensatedFillFactor }),
		table.Div[*LevelMetrics](),
		table.Count("tables", 6, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TablesMoved }),
		table.Literal[*LevelMetrics](" "),
		table.Bytes("size", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.TableBytesMoved }),
		table.Div[*LevelMetrics](),
		table.Bytes("top", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.MultiLevel.TableBytesInTop }),
		table.Literal[*LevelMetrics](" "),
		table.Bytes("in", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.MultiLevel.TableBytesIn }),
		table.Literal[*LevelMetrics](" "),
		table.Bytes("read", 5, table.AlignRight, func(m *LevelMetrics) uint64 { return m.MultiLevel.TableBytesRead }),
	)
	levelCompactionMetricsTableBottomDivider = strings.Repeat("-", compactionLevelMetricsTable.CumulativeFieldWidth)
	compactionKindTable                      = table.Define(
		table.String("kind", 10, table.AlignRight, func(p pair[string, int64]) string { return p.k }),
		table.CountInt64("count", 10, table.AlignRight, func(p pair[string, int64]) int64 { return p.v }),
	)
)

type pair[k, v any] struct {
	k k
	v v
}

// String pretty-prints the metrics as below:
//
//	      |                             |                |       |   ingested   |     moved    |    written   |       |    amp   |     multilevel
//	level | tables  size val-bl vtables | score  uc    c |   in  | tables  size | tables  size | tables  size |  read |   r   w  |    top   in  read
//	------+-----------------------------+----------------+-------+--------------+--------------+--------------+-------+----------+------------------
//	    0 |   101   102B     0B     101 | 1.10 2.10 0.30 |  104B |   112   104B |   113   106B |   221   217B |  107B |   1 2.09 |  104B  104B  104B
//	    1 |   201   202B     0B     201 | 1.20 2.20 0.60 |  204B |   212   204B |   213   206B |   421   417B |  207B |   2 2.04 |  204B  204B  204B
//	    2 |   301   302B     0B     301 | 1.30 2.30 0.90 |  304B |   312   304B |   313   306B |   621   617B |  307B |   3 2.03 |  304B  304B  304B
//	    3 |   401   402B     0B     401 | 1.40 2.40 1.20 |  404B |   412   404B |   413   406B |   821   817B |  407B |   4 2.02 |  404B  404B  404B
//	    4 |   501   502B     0B     501 | 1.50 2.50 1.50 |  504B |   512   504B |   513   506B |  1.0K  1017B |  507B |   5 2.02 |  504B  504B  504B
//	    5 |   601   602B     0B     601 | 1.60 2.60 1.80 |  604B |   612   604B |   613   606B |  1.2K  1.2KB |  607B |   6 2.01 |  604B  604B  604B
//	    6 |   701   702B     0B     701 |    - 2.70 2.10 |  704B |   712   704B |   713   706B |  1.4K  1.4KB |  707B |   7 2.01 |  704B  704B  704B
//	total |  2.8K  2.7KB     0B    2.8K |    -    -    - | 2.8KB |  2.9K  2.8KB |  2.9K  2.8KB |  5.7K  8.4KB | 2.8KB |  28 3.00 | 2.8KB 2.8KB 2.8KB
//	------------------------------------------------------------------------------------------------------------------------------------------------
//	WAL: 22 files (24B)  in: 25B  written: 26B (4% overhead)
//	Flushes: 8
//	Compactions: 5  estimated debt: 6B  in progress: 2 (7B)
//	             default: 27  delete: 28  elision: 29  move: 30  read: 31  tombstone-density: 16  rewrite: 32  copy: 33  multi-level: 34
//	MemTables: 12 (11B)  zombie: 14 (13B)
//	Zombie tables: 16 (15B, local: 30B)
//	Backing tables: 1 (2.0MB)
//	Virtual tables: 2807 (2.8KB)
//	Local tables size: 28B
//	Compression types:
//	Table stats: 31
//	Block cache: 2 entries (1B)  hit rate: 42.9%
//	Table cache: 18 entries (17B)  hit rate: 48.7%
//	Range key sets: 123  Tombstones: 456  Total missized tombstones encountered: 789
//	Snapshots: 4  earliest seq num: 1024
//	Table iters: 21
//	Filter utility: 47.4%
//	Ingestions: 27  as flushable: 36 (34B in 35 tables)
//	Cgo memory usage: 15KB  block cache: 9.0KB (data: 4.0KB, maps: 2.0KB, entries: 3.0KB)  memtables: 5.0KB
func (m *Metrics) String() string {
	wb := whiteboard.Make(92, levelMetricsTable.CumulativeFieldWidth)
	var total LevelMetrics
	for l := 0; l < numLevels; l++ {
		total.Add(&m.Levels[l])
	}
	// Compute total bytes-in as the bytes written to the WAL + bytes ingested.
	total.TableBytesIn = m.WAL.BytesWritten + total.TableBytesIngested
	// Add the total bytes-in to the total bytes-flushed. This is to account for
	// the bytes written to the log and bytes written externally and then
	// ingested.
	total.TableBytesFlushed += total.TableBytesIn
	total.Score = math.NaN()
	total.FillFactor = math.NaN()
	total.CompensatedFillFactor = math.NaN()

	// LSM level metrics.
	pos := wb.At(0, 0)
	pos = pos.WriteString(levelMetricsTableTopHeader).NewlineReturn()
	renderLevelMetrics := levelMetricsTable.RenderFunc(pos, table.RenderOptions{})
	for l := 0; l < numLevels; l++ {
		renderLevelMetrics(&m.Levels[l])
	}
	pos = renderLevelMetrics(&total)
	pos.Offset(-1, 0).WriteString("total")
	pos = pos.WriteString(levelMetricsTableBottomDivider).NewlineReturn()

	// Compaction level metrics.
	pos = pos.WriteString(levelCompactionMetricsTableTopHeader).NewlineReturn()
	renderCompactionLevelMetrics := compactionLevelMetricsTable.RenderFunc(pos, table.RenderOptions{})
	for l := 0; l < numLevels; l++ {
		renderCompactionLevelMetrics(&m.Levels[l])
	}
	pos = renderCompactionLevelMetrics(&total)
	pos.Offset(-1, 0).WriteString("total")

	kindTablePos := pos
	renderCompactionKind := compactionKindTable.RenderFunc(kindTablePos, table.RenderOptions{Orientation: table.Horizontally})
	renderCompactionKind(pair[string, int64]{k: "default", v: m.Compact.DefaultCount})
	renderCompactionKind(pair[string, int64]{k: "delete", v: m.Compact.DeleteOnlyCount})
	renderCompactionKind(pair[string, int64]{k: "elision", v: m.Compact.ElisionOnlyCount})
	renderCompactionKind(pair[string, int64]{k: "move", v: m.Compact.MoveCount})
	renderCompactionKind(pair[string, int64]{k: "read", v: m.Compact.ReadCount})
	renderCompactionKind(pair[string, int64]{k: "tombdense", v: m.Compact.TombstoneDensityCount})
	renderCompactionKind(pair[string, int64]{k: "rewrite", v: m.Compact.RewriteCount})
	renderCompactionKind(pair[string, int64]{k: "copy", v: m.Compact.CopyCount})
	pos = renderCompactionKind(pair[string, int64]{k: "multilvl", v: m.Compact.MultiLevelCount})
	dividerLen := max(compactionLevelMetricsTable.CumulativeFieldWidth, pos.Column())
	kindTablePos.Offset(-1, 0).RepeatByte(dividerLen, '-')
	return wb.String()
}

func hitRate(hits, misses int64) float64 {
	return percent(hits, hits+misses)
}

func percent(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return 100 * float64(numerator) / float64(denominator)
}

// StringForTests is identical to m.String() on 64-bit platforms. It is used to
// provide a platform-independent result for tests.
func (m *Metrics) StringForTests() string {
	mCopy := *m
	if math.MaxInt == math.MaxInt32 {
		// README: This is the difference in Sizeof(sstable.Reader{})) + Sizeof(blob.FileReader{})
		// between 64 and 32 bit platforms. See Metrics() in file_cache.go for more details.
		// This magic number must be updated if the sstable.Reader or blob.FileReader struct changes.
		// On 64-bit platforms, the size of the sstable.Reader struct is 616 bytes.
		// On 32-bit platforms, the size of the sstable.Reader struct is 496 bytes.
		// On 64-bit platforms, the size of the blob.FileReader struct is 88 bytes.
		// On 32-bit platforms, the size of the blob.FileReader struct is 56 bytes.
		// The difference is 616 - 496 + 88 - 56 = 152 bytes.
		const tableCacheSizeAdjustment = 152
		mCopy.FileCache.Size += mCopy.FileCache.Count * tableCacheSizeAdjustment
	}
	// Don't show cgo memory statistics as they can vary based on architecture,
	// invariants tag, etc.
	mCopy.manualMemory = manual.Metrics{}
	return redact.StringWithoutMarkers(&mCopy)
}

// levelMetricsDelta accumulates incremental ("delta") level metric updates
// (e.g. from compactions or flushes).
type levelMetricsDelta [manifest.NumLevels]*LevelMetrics

func (m *Metrics) updateLevelMetrics(updates levelMetricsDelta) {
	for i, u := range updates {
		if u != nil {
			m.Levels[i].Add(u)
		}
	}
}

// humanizeFloat formats a float64 value as a string. It shows up to two
// decimals, depending on the target length. NaN is shown as "-".
func humanizeFloat(v float64, targetLength int) redact.SafeString {
	if math.IsNaN(v) {
		return "-"
	}
	// We treat 0 specially. Values near zero will show up as 0.00.
	if v == 0 {
		return "0"
	}
	res := fmt.Sprintf("%.2f", v)
	if len(res) <= targetLength {
		return redact.SafeString(res)
	}
	if len(res) == targetLength+1 {
		return redact.SafeString(fmt.Sprintf("%.1f", v))
	}
	return redact.SafeString(fmt.Sprintf("%.0f", v))
}
