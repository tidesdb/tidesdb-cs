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
    /// Global memory limit in bytes (default: 0 = auto, 50% of system RAM; minimum: 5% of system RAM).
    /// </summary>
    public ulong MaxMemoryUsage { get; init; } = 0;

    /// <summary>
    /// Flag to determine if debug logging should be written to a file (default: false).
    /// </summary>
    public bool LogToFile { get; init; } = false;

    /// <summary>
    /// Log file truncation threshold in bytes (0 = no truncation, default: 0).
    /// </summary>
    public ulong LogTruncationAt { get; init; } = 0;

    /// <summary>
    /// Enable unified memtable mode (default: false = per-CF memtables).
    /// </summary>
    public bool UnifiedMemtable { get; init; } = false;

    /// <summary>
    /// Unified memtable write buffer size in bytes (default: 0 = auto).
    /// </summary>
    public ulong UnifiedMemtableWriteBufferSize { get; init; } = 0;

    /// <summary>
    /// Skip list max level for unified memtable (default: 0 = 12).
    /// </summary>
    public int UnifiedMemtableSkipListMaxLevel { get; init; } = 0;

    /// <summary>
    /// Skip list probability for unified memtable (default: 0 = 0.25).
    /// </summary>
    public float UnifiedMemtableSkipListProbability { get; init; } = 0;

    /// <summary>
    /// Sync mode for unified WAL (default: None).
    /// </summary>
    public SyncMode UnifiedMemtableSyncMode { get; init; } = SyncMode.None;

    /// <summary>
    /// Sync interval for unified WAL in microseconds (default: 0).
    /// </summary>
    public ulong UnifiedMemtableSyncIntervalUs { get; init; } = 0;

    /// <summary>
    /// Object store behavior configuration (null = object store disabled).
    /// Setting this automatically enables unified memtable mode.
    /// </summary>
    public ObjectStoreConfig? ObjectStoreConfig { get; init; }

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
    /// Compact less aggressively in object store mode (default: false).
    /// </summary>
    public bool ObjectLazyCompaction { get; init; } = false;

    /// <summary>
    /// Download all inputs before merge in object store mode (default: true).
    /// </summary>
    public bool ObjectPrefetchCompaction { get; init; } = true;

    /// <summary>
    /// Creates a default column family configuration.
    /// </summary>
    public static ColumnFamilyConfig Default => new();
}

/// <summary>
/// Configuration for object store mode behavior.
/// </summary>
public sealed class ObjectStoreConfig
{
    /// <summary>
    /// Object store connector type (default: Filesystem).
    /// </summary>
    public ObjectStoreConnectorType ConnectorType { get; init; } = ObjectStoreConnectorType.Filesystem;

    /// <summary>
    /// Root directory for the filesystem connector.
    /// Required when ConnectorType is Filesystem.
    /// </summary>
    public string? FsRootDir { get; init; }

    /// <summary>
    /// S3 endpoint (e.g., "s3.amazonaws.com" or "localhost:9000" for MinIO).
    /// Required when ConnectorType is S3.
    /// </summary>
    public string? S3Endpoint { get; init; }

    /// <summary>
    /// S3 bucket name. Required when ConnectorType is S3.
    /// </summary>
    public string? S3Bucket { get; init; }

    /// <summary>
    /// S3 key prefix (optional, e.g., "production/db1/").
    /// </summary>
    public string? S3KeyPrefix { get; init; }

    /// <summary>
    /// S3 access key. Required when ConnectorType is S3.
    /// </summary>
    public string? S3AccessKey { get; init; }

    /// <summary>
    /// S3 secret key. Required when ConnectorType is S3.
    /// </summary>
    public string? S3SecretKey { get; init; }

    /// <summary>
    /// S3 region (e.g., "us-east-1"). Can be null for MinIO.
    /// </summary>
    public string? S3Region { get; init; }

    /// <summary>
    /// Use SSL (HTTPS) for S3 connections (default: true).
    /// </summary>
    public bool S3UseSsl { get; init; } = true;

    /// <summary>
    /// Use path-style URLs for S3 (default: false for AWS, set true for MinIO).
    /// </summary>
    public bool S3UsePathStyle { get; init; } = false;

    /// <summary>
    /// Local directory for cached SSTable files (null = use db_path).
    /// </summary>
    public string? LocalCachePath { get; init; }

    /// <summary>
    /// Maximum local cache size in bytes (default: 0 = unlimited).
    /// </summary>
    public ulong LocalCacheMaxBytes { get; init; } = 0;

    /// <summary>
    /// Cache downloaded files locally (default: true).
    /// </summary>
    public bool CacheOnRead { get; init; } = true;

    /// <summary>
    /// Keep local copy after upload (default: true).
    /// </summary>
    public bool CacheOnWrite { get; init; } = true;

    /// <summary>
    /// Number of parallel upload threads (default: 4).
    /// </summary>
    public int MaxConcurrentUploads { get; init; } = 4;

    /// <summary>
    /// Number of parallel download threads (default: 8).
    /// </summary>
    public int MaxConcurrentDownloads { get; init; } = 8;

    /// <summary>
    /// Use multipart upload above this size in bytes (default: 64MB).
    /// </summary>
    public ulong MultipartThreshold { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// Chunk size for multipart uploads in bytes (default: 8MB).
    /// </summary>
    public ulong MultipartPartSize { get; init; } = 8 * 1024 * 1024;

    /// <summary>
    /// Upload MANIFEST after each compaction (default: true).
    /// </summary>
    public bool SyncManifestToObject { get; init; } = true;

    /// <summary>
    /// Upload closed WAL segments for replication (default: true).
    /// </summary>
    public bool ReplicateWal { get; init; } = true;

    /// <summary>
    /// Block flush until WAL is uploaded (default: false = background upload).
    /// </summary>
    public bool WalUploadSync { get; init; } = false;

    /// <summary>
    /// Sync active WAL when it grows by this many bytes (default: 1MB, 0 = off).
    /// </summary>
    public ulong WalSyncThresholdBytes { get; init; } = 1024 * 1024;

    /// <summary>
    /// Upload WAL after every txn commit for RPO=0 replication (default: false).
    /// </summary>
    public bool WalSyncOnCommit { get; init; } = false;

    /// <summary>
    /// Enable read-only replica mode (default: false).
    /// </summary>
    public bool ReplicaMode { get; init; } = false;

    /// <summary>
    /// MANIFEST poll interval for replica sync in microseconds (default: 5000000 = 5s).
    /// </summary>
    public ulong ReplicaSyncIntervalUs { get; init; } = 5_000_000;

    /// <summary>
    /// Replay WAL from object store for near-real-time reads on replicas (default: true).
    /// </summary>
    public bool ReplicaReplayWal { get; init; } = true;

    /// <summary>
    /// Creates a default object store configuration.
    /// </summary>
    public static ObjectStoreConfig Default => new();
}

/// <summary>
/// Object store connector types.
/// </summary>
public enum ObjectStoreConnectorType
{
    /// <summary>
    /// Filesystem-backed object store (for testing and local replication).
    /// </summary>
    Filesystem = 0,

    /// <summary>
    /// S3-compatible object store (AWS S3, MinIO, GCS with S3 compatibility).
    /// Requires TidesDB built with -DTIDESDB_WITH_S3=ON.
    /// </summary>
    S3 = 1
}
