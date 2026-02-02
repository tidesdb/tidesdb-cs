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
