// Package TidesDB
// Copyright (C) TidesDB
//
// Original Author: Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace TidesDB;

/// <summary>
/// Configuration for opening a TidesDB database.
/// </summary>
public class Config
{
    /// <summary>Path to the database directory.</summary>
    public string DbPath { get; set; } = "";

    /// <summary>Number of flush threads (default: 2).</summary>
    public int NumFlushThreads { get; set; } = 2;

    /// <summary>Number of compaction threads (default: 2).</summary>
    public int NumCompactionThreads { get; set; } = 2;

    /// <summary>Minimum log level to display (default: Info).</summary>
    public LogLevel LogLevel { get; set; } = LogLevel.Info;

    /// <summary>Size of the global block cache in bytes (default: 64MB).</summary>
    public ulong BlockCacheSize { get; set; } = 64 * 1024 * 1024;

    /// <summary>Maximum number of open SSTable structures (default: 256).</summary>
    public ulong MaxOpenSSTables { get; set; } = 256;

    /// <summary>
    /// Creates a default configuration.
    /// </summary>
    public static Config Default() => new();
}

/// <summary>
/// Configuration for a column family.
/// </summary>
public class ColumnFamilyConfig
{
    /// <summary>Size of write buffer / memtable flush threshold.</summary>
    public ulong WriteBufferSize { get; set; } = 64 * 1024 * 1024;

    /// <summary>Level size multiplier (default: 10).</summary>
    public ulong LevelSizeRatio { get; set; } = 10;

    /// <summary>Minimum number of LSM levels (default: 5).</summary>
    public int MinLevels { get; set; } = 5;

    /// <summary>Compaction dividing level offset (default: 2).</summary>
    public int DividingLevelOffset { get; set; } = 2;

    /// <summary>Values larger than this go to vlog (default: 512).</summary>
    public ulong KlogValueThreshold { get; set; } = 512;

    /// <summary>Compression algorithm (default: LZ4).</summary>
    public CompressionAlgorithm CompressionAlgorithm { get; set; } = CompressionAlgorithm.Lz4;

    /// <summary>Enable bloom filters (default: true).</summary>
    public bool EnableBloomFilter { get; set; } = true;

    /// <summary>Bloom filter false positive rate (default: 0.01).</summary>
    public double BloomFpr { get; set; } = 0.01;

    /// <summary>Enable block indexes (default: true).</summary>
    public bool EnableBlockIndexes { get; set; } = true;

    /// <summary>Index sample ratio (default: 1).</summary>
    public int IndexSampleRatio { get; set; } = 1;

    /// <summary>Block index prefix length (default: 16).</summary>
    public int BlockIndexPrefixLen { get; set; } = 16;

    /// <summary>Sync mode for durability (default: Interval).</summary>
    public SyncMode SyncMode { get; set; } = SyncMode.Interval;

    /// <summary>Sync interval in microseconds (default: 128000).</summary>
    public ulong SyncIntervalUs { get; set; } = 128000;

    /// <summary>Comparator name (default: "memcmp").</summary>
    public string ComparatorName { get; set; } = "";

    /// <summary>Skip list max level (default: 12).</summary>
    public int SkipListMaxLevel { get; set; } = 12;

    /// <summary>Skip list probability (default: 0.25).</summary>
    public float SkipListProbability { get; set; } = 0.25f;

    /// <summary>Default transaction isolation level.</summary>
    public IsolationLevel DefaultIsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

    /// <summary>Minimum free disk space required in bytes (default: 100MB).</summary>
    public ulong MinDiskSpace { get; set; } = 100 * 1024 * 1024;

    /// <summary>L1 file count trigger for compaction (default: 4).</summary>
    public int L1FileCountTrigger { get; set; } = 4;

    /// <summary>L0 queue stall threshold (default: 20).</summary>
    public int L0QueueStallThreshold { get; set; } = 20;

    /// <summary>
    /// Creates a default column family configuration.
    /// </summary>
    public static ColumnFamilyConfig Default() => new();
}

/// <summary>
/// Statistics about a column family.
/// </summary>
public record Stats
{
    /// <summary>Number of LSM levels.</summary>
    public int NumLevels { get; init; }

    /// <summary>Size of the memtable in bytes.</summary>
    public ulong MemtableSize { get; init; }

    /// <summary>Sizes of each level in bytes.</summary>
    public ulong[] LevelSizes { get; init; } = Array.Empty<ulong>();

    /// <summary>Number of SSTables in each level.</summary>
    public int[] LevelNumSSTables { get; init; } = Array.Empty<int>();

    /// <summary>Column family configuration.</summary>
    public ColumnFamilyConfig? Config { get; init; }
}

/// <summary>
/// Statistics about the block cache.
/// </summary>
public class CacheStats
{
    /// <summary>Whether the block cache is enabled.</summary>
    public bool Enabled { get; init; }

    /// <summary>Total number of cached entries.</summary>
    public ulong TotalEntries { get; init; }

    /// <summary>Total bytes used by the cache.</summary>
    public ulong TotalBytes { get; init; }

    /// <summary>Number of cache hits.</summary>
    public ulong Hits { get; init; }

    /// <summary>Number of cache misses.</summary>
    public ulong Misses { get; init; }

    /// <summary>Cache hit rate (0.0 to 1.0).</summary>
    public double HitRate { get; init; }

    /// <summary>Number of cache partitions.</summary>
    public ulong NumPartitions { get; init; }
}
