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

using System.Runtime.InteropServices;
using System.Text;

namespace TidesDB;

/// <summary>
/// TidesDB is a high-performance embedded key-value storage engine.
/// </summary>
public class TidesDB : IDisposable
{
    private IntPtr _handle;
    private bool _disposed;

    private TidesDB(IntPtr handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Opens a TidesDB database with the specified configuration.
    /// </summary>
    /// <param name="config">The database configuration.</param>
    /// <returns>A new TidesDB instance.</returns>
    public static TidesDB Open(Config config)
    {
        var dbPathPtr = Marshal.StringToHGlobalAnsi(config.DbPath);
        try
        {
            var nativeConfig = new Native.tidesdb_config_t
            {
                db_path = dbPathPtr,
                num_flush_threads = config.NumFlushThreads,
                num_compaction_threads = config.NumCompactionThreads,
                log_level = (int)config.LogLevel,
                block_cache_size = (nuint)config.BlockCacheSize,
                max_open_sstables = (nuint)config.MaxOpenSSTables
            };

            var result = Native.tidesdb_open(ref nativeConfig, out var dbPtr);
            TidesDBException.CheckResult(result, "failed to open database");

            return new TidesDB(dbPtr);
        }
        finally
        {
            Marshal.FreeHGlobal(dbPathPtr);
        }
    }

    /// <summary>
    /// Closes the database.
    /// </summary>
    public void Close()
    {
        if (!_disposed && _handle != IntPtr.Zero)
        {
            var result = Native.tidesdb_close(_handle);
            _handle = IntPtr.Zero;
            _disposed = true;
            TidesDBException.CheckResult(result, "failed to close database");
        }
    }

    /// <summary>
    /// Creates a new column family with the specified configuration.
    /// </summary>
    /// <param name="name">The column family name.</param>
    /// <param name="config">The column family configuration (optional, uses defaults if null).</param>
    public void CreateColumnFamily(string name, ColumnFamilyConfig? config = null)
    {
        ThrowIfDisposed();
        config ??= ColumnFamilyConfig.Default();

        var nativeConfig = ToNativeConfig(config);
        var result = Native.tidesdb_create_column_family(_handle, name, ref nativeConfig);
        TidesDBException.CheckResult(result, "failed to create column family");
    }

    /// <summary>
    /// Drops a column family and all associated data.
    /// </summary>
    /// <param name="name">The column family name.</param>
    public void DropColumnFamily(string name)
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_drop_column_family(_handle, name);
        TidesDBException.CheckResult(result, "failed to drop column family");
    }

    /// <summary>
    /// Gets a column family by name.
    /// </summary>
    /// <param name="name">The column family name.</param>
    /// <returns>The column family.</returns>
    public ColumnFamily GetColumnFamily(string name)
    {
        ThrowIfDisposed();
        var cfPtr = Native.tidesdb_get_column_family(_handle, name);
        if (cfPtr == IntPtr.Zero)
        {
            throw new TidesDBException(ErrorCode.NotFound, $"column family not found: {name}");
        }
        return new ColumnFamily(cfPtr, name);
    }

    /// <summary>
    /// Lists all column families in the database.
    /// </summary>
    /// <returns>An array of column family names.</returns>
    public string[] ListColumnFamilies()
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_list_column_families(_handle, out var namesPtr, out var count);
        TidesDBException.CheckResult(result, "failed to list column families");

        if (count == 0 || namesPtr == IntPtr.Zero)
        {
            return Array.Empty<string>();
        }

        try
        {
            var names = new string[count];
            for (int i = 0; i < count; i++)
            {
                var strPtr = Marshal.ReadIntPtr(namesPtr, i * IntPtr.Size);
                names[i] = Marshal.PtrToStringAnsi(strPtr) ?? "";
                Marshal.FreeHGlobal(strPtr);
            }
            return names;
        }
        finally
        {
            Marshal.FreeHGlobal(namesPtr);
        }
    }

    /// <summary>
    /// Begins a new transaction with the default isolation level.
    /// </summary>
    /// <returns>A new transaction.</returns>
    public Transaction BeginTransaction()
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_txn_begin(_handle, out var txnPtr);
        TidesDBException.CheckResult(result, "failed to begin transaction");
        return new Transaction(txnPtr);
    }

    /// <summary>
    /// Begins a new transaction with the specified isolation level.
    /// </summary>
    /// <param name="isolation">The isolation level.</param>
    /// <returns>A new transaction.</returns>
    public Transaction BeginTransactionWithIsolation(IsolationLevel isolation)
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_txn_begin_with_isolation(_handle, (int)isolation, out var txnPtr);
        TidesDBException.CheckResult(result, "failed to begin transaction with isolation");
        return new Transaction(txnPtr);
    }

    /// <summary>
    /// Gets statistics about the block cache.
    /// </summary>
    /// <returns>Cache statistics.</returns>
    public CacheStats GetCacheStats()
    {
        ThrowIfDisposed();
        var nativeStats = new Native.tidesdb_cache_stats_t();
        var result = Native.tidesdb_get_cache_stats(_handle, ref nativeStats);
        TidesDBException.CheckResult(result, "failed to get cache stats");

        return new CacheStats
        {
            Enabled = nativeStats.enabled != 0,
            TotalEntries = (ulong)nativeStats.total_entries,
            TotalBytes = (ulong)nativeStats.total_bytes,
            Hits = nativeStats.hits,
            Misses = nativeStats.misses,
            HitRate = nativeStats.hit_rate,
            NumPartitions = (ulong)nativeStats.num_partitions
        };
    }

    private static unsafe Native.tidesdb_column_family_config_t ToNativeConfig(ColumnFamilyConfig config)
    {
        var nativeConfig = new Native.tidesdb_column_family_config_t
        {
            write_buffer_size = (nuint)config.WriteBufferSize,
            level_size_ratio = (nuint)config.LevelSizeRatio,
            min_levels = config.MinLevels,
            dividing_level_offset = config.DividingLevelOffset,
            klog_value_threshold = (nuint)config.KlogValueThreshold,
            compression_algo = (int)config.CompressionAlgorithm,
            enable_bloom_filter = config.EnableBloomFilter ? 1 : 0,
            bloom_fpr = config.BloomFpr,
            enable_block_indexes = config.EnableBlockIndexes ? 1 : 0,
            index_sample_ratio = config.IndexSampleRatio,
            block_index_prefix_len = config.BlockIndexPrefixLen,
            sync_mode = (int)config.SyncMode,
            sync_interval_us = config.SyncIntervalUs,
            skip_list_max_level = config.SkipListMaxLevel,
            skip_list_probability = config.SkipListProbability,
            default_isolation_level = (int)config.DefaultIsolationLevel,
            min_disk_space = config.MinDiskSpace,
            l1_file_count_trigger = config.L1FileCountTrigger,
            l0_queue_stall_threshold = config.L0QueueStallThreshold
        };

        if (!string.IsNullOrEmpty(config.ComparatorName))
        {
            var bytes = Encoding.ASCII.GetBytes(config.ComparatorName);
            var len = Math.Min(bytes.Length, Native.TDB_MAX_COMPARATOR_NAME - 1);
            for (int i = 0; i < len; i++)
            {
                nativeConfig.comparator_name[i] = bytes[i];
            }
            nativeConfig.comparator_name[len] = 0;
        }

        return nativeConfig;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(TidesDB));
        }
    }

    /// <summary>
    /// Releases the database resources.
    /// </summary>
    public void Dispose()
    {
        Close();
        GC.SuppressFinalize(this);
    }

    ~TidesDB()
    {
        Close();
    }
}
