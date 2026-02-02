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
/// Configuration for opening a TidesDB instance.
/// </summary>
public sealed class Config
{
    /// <summary>
    /// Path to the database directory.
    /// </summary>
    public required string DbPath { get; init; }

    /// <summary>
    /// Number of flush threads (default: 2).
    /// </summary>
    public int NumFlushThreads { get; init; } = 2;

    /// <summary>
    /// Number of compaction threads (default: 2).
    /// </summary>
    public int NumCompactionThreads { get; init; } = 2;

    /// <summary>
    /// Logging level (default: Info).
    /// </summary>
    public LogLevel LogLevel { get; init; } = LogLevel.Info;

    /// <summary>
    /// Block cache size in bytes (default: 64MB).
    /// </summary>
    public ulong BlockCacheSize { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// Maximum number of open SSTables (default: 256).
    /// </summary>
    public ulong MaxOpenSstables { get; init; } = 256;

    /// <summary>
    /// Flag to determine if debug logging should be written to a file (default: false).
    /// </summary>
    public bool LogToFile { get; init; } = false;

    /// <summary>
    /// Log file truncation threshold in bytes (0 = no truncation, default: 0).
    /// </summary>
    public ulong LogTruncationAt { get; init; } = 0;

    /// <summary>
    /// Creates a default configuration with the specified database path.
    /// </summary>
    public static Config Default(string dbPath) => new()
    {
        DbPath = dbPath
    };
}

/// <summary>
/// Configuration for a column family.
/// </summary>
public sealed class ColumnFamilyConfig
{
    /// <summary>
    /// Write buffer size in bytes (memtable flush threshold).
    /// </summary>
    public ulong WriteBufferSize { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// Level size ratio (default: 10).
    /// </summary>
    public ulong LevelSizeRatio { get; init; } = 10;

    /// <summary>
    /// Minimum number of LSM levels (default: 5).
    /// </summary>
    public int MinLevels { get; init; } = 5;

    /// <summary>
    /// Compaction dividing level offset (default: 2).
    /// </summary>
    public int DividingLevelOffset { get; init; } = 2;

    /// <summary>
    /// Values larger than this threshold go to vlog (default: 512).
    /// </summary>
    public ulong KlogValueThreshold { get; init; } = 512;

    /// <summary>
    /// Compression algorithm (default: Lz4).
    /// </summary>
    public CompressionAlgorithm CompressionAlgorithm { get; init; } = CompressionAlgorithm.Lz4;

    /// <summary>
    /// Enable bloom filters (default: true).
    /// </summary>
    public bool EnableBloomFilter { get; init; } = true;

    /// <summary>
    /// Bloom filter false positive rate (default: 0.01).
    /// </summary>
    public double BloomFpr { get; init; } = 0.01;

    /// <summary>
    /// Enable block indexes (default: true).
    /// </summary>
    public bool EnableBlockIndexes { get; init; } = true;

    /// <summary>
    /// Index sample ratio (default: 1).
    /// </summary>
    public int IndexSampleRatio { get; init; } = 1;

    /// <summary>
    /// Block index prefix length (default: 16).
    /// </summary>
    public int BlockIndexPrefixLen { get; init; } = 16;

    /// <summary>
    /// Sync mode for durability (default: Full).
    /// </summary>
    public SyncMode SyncMode { get; init; } = SyncMode.Full;

    /// <summary>
    /// Sync interval in microseconds (only for SyncMode.Interval).
    /// </summary>
    public ulong SyncIntervalUs { get; init; } = 1_000_000;

    /// <summary>
    /// Comparator name (empty for default "memcmp").
    /// </summary>
    public string ComparatorName { get; init; } = "";

    /// <summary>
    /// Skip list max level (default: 12).
    /// </summary>
    public int SkipListMaxLevel { get; init; } = 12;

    /// <summary>
    /// Skip list probability (default: 0.25).
    /// </summary>
    public float SkipListProbability { get; init; } = 0.25f;

    /// <summary>
    /// Default transaction isolation level (default: ReadCommitted).
    /// </summary>
    public IsolationLevel DefaultIsolationLevel { get; init; } = IsolationLevel.ReadCommitted;

    /// <summary>
    /// Minimum disk space required in bytes (default: 100MB).
    /// </summary>
    public ulong MinDiskSpace { get; init; } = 100 * 1024 * 1024;

    /// <summary>
    /// L1 file count trigger for compaction (default: 4).
    /// </summary>
    public int L1FileCountTrigger { get; init; } = 4;

    /// <summary>
    /// L0 queue stall threshold (default: 20).
    /// </summary>
    public int L0QueueStallThreshold { get; init; } = 20;

    /// <summary>
    /// Use B+tree format for klog (default: false = block-based).
    /// </summary>
    public bool UseBtree { get; init; } = false;

    /// <summary>
    /// Creates a default column family configuration.
    /// </summary>
    public static ColumnFamilyConfig Default => new();
}
