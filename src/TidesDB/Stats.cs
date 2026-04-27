// Copyright (C) TidesDB
//
// Original Author: Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace TidesDB;

/// <summary>
/// Statistics about a column family.
/// </summary>
public sealed record Stats
{
    /// <summary>
    /// Number of LSM levels.
    /// </summary>
    public int NumLevels { get; init; }

    /// <summary>
    /// Current memtable size in bytes.
    /// </summary>
    public ulong MemtableSize { get; init; }

    /// <summary>
    /// Size of each level in bytes.
    /// </summary>
    public ulong[] LevelSizes { get; init; } = [];

    /// <summary>
    /// Number of SSTables in each level.
    /// </summary>
    public int[] LevelNumSstables { get; init; } = [];

    /// <summary>
    /// Number of keys in each level.
    /// </summary>
    public ulong[] LevelKeyCounts { get; init; } = [];

    /// <summary>
    /// Column family configuration.
    /// </summary>
    public ColumnFamilyConfig? Config { get; init; }

    /// <summary>
    /// Total keys across memtable and all SSTables.
    /// </summary>
    public ulong TotalKeys { get; init; }

    /// <summary>
    /// Total data size (klog + vlog) in bytes.
    /// </summary>
    public ulong TotalDataSize { get; init; }

    /// <summary>
    /// Estimated average key size in bytes.
    /// </summary>
    public double AvgKeySize { get; init; }

    /// <summary>
    /// Estimated average value size in bytes.
    /// </summary>
    public double AvgValueSize { get; init; }

    /// <summary>
    /// Read amplification factor (point lookup cost).
    /// </summary>
    public double ReadAmp { get; init; }

    /// <summary>
    /// Block cache hit rate (0.0 to 1.0).
    /// </summary>
    public double HitRate { get; init; }

    /// <summary>
    /// Whether column family uses B+tree format.
    /// </summary>
    public bool UseBtree { get; init; }

    /// <summary>
    /// Total B+tree nodes across all SSTables (only populated if UseBtree=true).
    /// </summary>
    public ulong BtreeTotalNodes { get; init; }

    /// <summary>
    /// Maximum tree height across all SSTables (only populated if UseBtree=true).
    /// </summary>
    public uint BtreeMaxHeight { get; init; }

    /// <summary>
    /// Average tree height across all SSTables (only populated if UseBtree=true).
    /// </summary>
    public double BtreeAvgHeight { get; init; }

    /// <summary>
    /// Sum of tombstone counts across every SSTable in the column family.
    /// </summary>
    public ulong TotalTombstones { get; init; }

    /// <summary>
    /// Ratio of total tombstones to total keys (0.0 if no keys). Range [0.0, 1.0].
    /// </summary>
    public double TombstoneRatio { get; init; }

    /// <summary>
    /// Tombstone count per level (parallels <see cref="LevelKeyCounts"/>).
    /// </summary>
    public ulong[] LevelTombstoneCounts { get; init; } = [];

    /// <summary>
    /// Worst per-SSTable tombstone density observed in the column family. Range [0.0, 1.0].
    /// </summary>
    public double MaxSstDensity { get; init; }

    /// <summary>
    /// 1-based level index where <see cref="MaxSstDensity"/> was observed (0 if none).
    /// </summary>
    public int MaxSstDensityLevel { get; init; }
}

/// <summary>
/// Aggregate statistics across the entire database instance.
/// </summary>
public sealed class DbStats
{
    /// <summary>
    /// Number of column families.
    /// </summary>
    public int NumColumnFamilies { get; init; }

    /// <summary>
    /// System total memory.
    /// </summary>
    public ulong TotalMemory { get; init; }

    /// <summary>
    /// System available memory at open time.
    /// </summary>
    public ulong AvailableMemory { get; init; }

    /// <summary>
    /// Resolved memory limit (auto or configured).
    /// </summary>
    public ulong ResolvedMemoryLimit { get; init; }

    /// <summary>
    /// Current memory pressure (0=normal, 1=elevated, 2=high, 3=critical).
    /// </summary>
    public int MemoryPressureLevel { get; init; }

    /// <summary>
    /// Number of pending flush operations (queued + in-flight).
    /// </summary>
    public int FlushPendingCount { get; init; }

    /// <summary>
    /// Total bytes in active memtables across all CFs.
    /// </summary>
    public long TotalMemtableBytes { get; init; }

    /// <summary>
    /// Total immutable memtables across all CFs.
    /// </summary>
    public int TotalImmutableCount { get; init; }

    /// <summary>
    /// Total SSTables across all CFs and levels.
    /// </summary>
    public int TotalSstableCount { get; init; }

    /// <summary>
    /// Total data size (klog + vlog) across all CFs.
    /// </summary>
    public ulong TotalDataSizeBytes { get; init; }

    /// <summary>
    /// Number of currently open SSTable file handles.
    /// </summary>
    public int NumOpenSstables { get; init; }

    /// <summary>
    /// Current global sequence number.
    /// </summary>
    public ulong GlobalSeq { get; init; }

    /// <summary>
    /// Bytes held by in-flight transactions.
    /// </summary>
    public long TxnMemoryBytes { get; init; }

    /// <summary>
    /// Number of pending compaction tasks.
    /// </summary>
    public ulong CompactionQueueSize { get; init; }

    /// <summary>
    /// Number of pending flush tasks in queue.
    /// </summary>
    public ulong FlushQueueSize { get; init; }

    /// <summary>
    /// Whether unified memtable mode is active.
    /// </summary>
    public bool UnifiedMemtableEnabled { get; init; }

    /// <summary>
    /// Bytes in unified active memtable.
    /// </summary>
    public long UnifiedMemtableBytes { get; init; }

    /// <summary>
    /// Number of unified immutable memtables.
    /// </summary>
    public int UnifiedImmutableCount { get; init; }

    /// <summary>
    /// Whether unified memtable is currently flushing/rotating.
    /// </summary>
    public bool UnifiedIsFlushing { get; init; }

    /// <summary>
    /// Next CF index to be assigned in unified mode.
    /// </summary>
    public uint UnifiedNextCfIndex { get; init; }

    /// <summary>
    /// Current unified WAL generation counter.
    /// </summary>
    public ulong UnifiedWalGeneration { get; init; }

    /// <summary>
    /// Whether object store mode is active.
    /// </summary>
    public bool ObjectStoreEnabled { get; init; }

    /// <summary>
    /// Connector name ("s3", "gcs", "fs", etc.), or null if not using object store.
    /// </summary>
    public string? ObjectStoreConnector { get; init; }

    /// <summary>
    /// Current local file cache usage in bytes.
    /// </summary>
    public ulong LocalCacheBytesUsed { get; init; }

    /// <summary>
    /// Configured maximum local cache size in bytes.
    /// </summary>
    public ulong LocalCacheBytesMax { get; init; }

    /// <summary>
    /// Number of files tracked in local cache.
    /// </summary>
    public int LocalCacheNumFiles { get; init; }

    /// <summary>
    /// Highest WAL generation confirmed uploaded.
    /// </summary>
    public ulong LastUploadedGeneration { get; init; }

    /// <summary>
    /// Number of pending upload jobs in the queue.
    /// </summary>
    public ulong UploadQueueDepth { get; init; }

    /// <summary>
    /// Lifetime count of objects uploaded to object store.
    /// </summary>
    public ulong TotalUploads { get; init; }

    /// <summary>
    /// Lifetime count of permanently failed uploads (after all retries).
    /// </summary>
    public ulong TotalUploadFailures { get; init; }

    /// <summary>
    /// Whether running in read-only replica mode.
    /// </summary>
    public bool ReplicaMode { get; init; }
}

/// <summary>
/// Statistics about the block cache.
/// </summary>
public sealed class CacheStats
{
    /// <summary>
    /// Whether the block cache is enabled.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Total number of cached entries.
    /// </summary>
    public ulong TotalEntries { get; init; }

    /// <summary>
    /// Total bytes used by the cache.
    /// </summary>
    public ulong TotalBytes { get; init; }

    /// <summary>
    /// Number of cache hits.
    /// </summary>
    public ulong Hits { get; init; }

    /// <summary>
    /// Number of cache misses.
    /// </summary>
    public ulong Misses { get; init; }

    /// <summary>
    /// Hit rate (0.0 to 1.0).
    /// </summary>
    public double HitRate { get; init; }

    /// <summary>
    /// Number of cache partitions.
    /// </summary>
    public ulong NumPartitions { get; init; }
}
