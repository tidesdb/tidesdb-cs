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

using System.Runtime.InteropServices;
using System.Text;
using TidesDB.Native;

namespace TidesDB;

/// <summary>
/// TidesDB database instance.
/// </summary>
public sealed class TidesDb : IDisposable
{
    private nint _handle;
    private bool _disposed;
    private nint _dbPathPtr;

    private TidesDb(nint handle, nint dbPathPtr)
    {
        _handle = handle;
        _dbPathPtr = dbPathPtr;
    }

    /// <summary>
    /// Opens a TidesDB instance with the given configuration.
    /// </summary>
    /// <param name="config">The database configuration.</param>
    /// <returns>A new TidesDB instance.</returns>
    public static TidesDb Open(Config config)
    {
        var dbPathPtr = Marshal.StringToHGlobalAnsi(config.DbPath);
        
        var nativeConfig = new NativeConfig
        {
            DbPath = dbPathPtr,
            NumFlushThreads = config.NumFlushThreads,
            NumCompactionThreads = config.NumCompactionThreads,
            LogLevel = (int)config.LogLevel,
            BlockCacheSize = (nuint)config.BlockCacheSize,
            MaxOpenSstables = (nuint)config.MaxOpenSstables,
            LogToFile = config.LogToFile ? 1 : 0,
            LogTruncationAt = (nuint)config.LogTruncationAt
        };

        var result = NativeMethods.tidesdb_open(ref nativeConfig, out var dbHandle);
        if (result != 0)
        {
            Marshal.FreeHGlobal(dbPathPtr);
            throw new TidesDBException(result, "failed to open database");
        }

        return new TidesDb(dbHandle, dbPathPtr);
    }

    /// <summary>
    /// Creates a new column family with the given configuration.
    /// </summary>
    /// <param name="name">The column family name.</param>
    /// <param name="config">The column family configuration.</param>
    public void CreateColumnFamily(string name, ColumnFamilyConfig? config = null)
    {
        ThrowIfDisposed();
        config ??= ColumnFamilyConfig.Default;

        var nativeConfig = CreateNativeColumnFamilyConfig(config);
        var result = NativeMethods.tidesdb_create_column_family(_handle, name, ref nativeConfig);
        TidesDBException.ThrowIfError(result, "failed to create column family");
    }

    /// <summary>
    /// Drops a column family and all associated data.
    /// </summary>
    /// <param name="name">The column family name.</param>
    public void DropColumnFamily(string name)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_drop_column_family(_handle, name);
        TidesDBException.ThrowIfError(result, "failed to drop column family");
    }

    /// <summary>
    /// Clones a column family, creating a complete copy with a new name.
    /// </summary>
    /// <param name="sourceName">The source column family name.</param>
    /// <param name="destName">The destination column family name.</param>
    public void CloneColumnFamily(string sourceName, string destName)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_clone_column_family(_handle, sourceName, destName);
        TidesDBException.ThrowIfError(result, "failed to clone column family");
    }

    /// <summary>
    /// Renames a column family atomically.
    /// </summary>
    /// <param name="oldName">The current column family name.</param>
    /// <param name="newName">The new column family name.</param>
    public void RenameColumnFamily(string oldName, string newName)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_rename_column_family(_handle, oldName, newName);
        TidesDBException.ThrowIfError(result, "failed to rename column family");
    }

    /// <summary>
    /// Creates an on-disk backup of the database without blocking reads/writes.
    /// </summary>
    /// <param name="dir">The backup directory path. Must be non-existent or empty.</param>
    public void Backup(string dir)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_backup(_handle, dir);
        TidesDBException.ThrowIfError(result, "failed to backup database");
    }

    /// <summary>
    /// Creates a lightweight, near-instant snapshot of the database using hard links.
    /// </summary>
    /// <param name="checkpointDir">The checkpoint directory path. Must be non-existent or empty.</param>
    public void Checkpoint(string checkpointDir)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_checkpoint(_handle, checkpointDir);
        TidesDBException.ThrowIfError(result, "failed to checkpoint database");
    }

    /// <summary>
    /// Gets a column family by name.
    /// </summary>
    /// <param name="name">The column family name.</param>
    /// <returns>The column family, or null if not found.</returns>
    public ColumnFamily? GetColumnFamily(string name)
    {
        ThrowIfDisposed();
        var cfHandle = NativeMethods.tidesdb_get_column_family(_handle, name);
        return cfHandle == nint.Zero ? null : new ColumnFamily(cfHandle);
    }

    /// <summary>
    /// Lists all column families in the database.
    /// </summary>
    /// <returns>An array of column family names.</returns>
    public string[] ListColumnFamilies()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_list_column_families(_handle, out var namesPtr, out var count);
        TidesDBException.ThrowIfError(result, "failed to list column families");

        if (count == 0)
        {
            return [];
        }

        var names = new string[count];
        for (int i = 0; i < count; i++)
        {
            var namePtr = Marshal.ReadIntPtr(namesPtr, i * nint.Size);
            names[i] = Marshal.PtrToStringAnsi(namePtr) ?? "";
            NativeMethods.tidesdb_free(namePtr);
        }
        NativeMethods.tidesdb_free(namesPtr);

        return names;
    }

    /// <summary>
    /// Begins a new transaction with default isolation level.
    /// </summary>
    /// <returns>A new transaction.</returns>
    public Transaction BeginTransaction()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_txn_begin(_handle, out var txnHandle);
        TidesDBException.ThrowIfError(result, "failed to begin transaction");
        return new Transaction(txnHandle);
    }

    /// <summary>
    /// Begins a new transaction with the specified isolation level.
    /// </summary>
    /// <param name="isolation">The isolation level.</param>
    /// <returns>A new transaction.</returns>
    public Transaction BeginTransaction(IsolationLevel isolation)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_txn_begin_with_isolation(_handle, (int)isolation, out var txnHandle);
        TidesDBException.ThrowIfError(result, "failed to begin transaction with isolation");
        return new Transaction(txnHandle);
    }

    /// <summary>
    /// Registers a custom comparator with the database.
    /// </summary>
    /// <param name="name">The comparator name.</param>
    /// <param name="ctxStr">Optional context string.</param>
    public void RegisterComparator(string name, string? ctxStr = null)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_register_comparator(_handle, name, nint.Zero, ctxStr, nint.Zero);
        TidesDBException.ThrowIfError(result, "failed to register comparator");
    }

    /// <summary>
    /// Gets statistics about the block cache.
    /// </summary>
    /// <returns>Cache statistics.</returns>
    public CacheStats GetCacheStats()
    {
        ThrowIfDisposed();
        var nativeStats = new NativeCacheStats();
        var result = NativeMethods.tidesdb_get_cache_stats(_handle, ref nativeStats);
        TidesDBException.ThrowIfError(result, "failed to get cache stats");

        return new CacheStats
        {
            Enabled = nativeStats.Enabled != 0,
            TotalEntries = (ulong)nativeStats.TotalEntries,
            TotalBytes = (ulong)nativeStats.TotalBytes,
            Hits = nativeStats.Hits,
            Misses = nativeStats.Misses,
            HitRate = nativeStats.HitRate,
            NumPartitions = (ulong)nativeStats.NumPartitions
        };
    }

    private static unsafe NativeColumnFamilyConfig CreateNativeColumnFamilyConfig(ColumnFamilyConfig config)
    {
        var nativeConfig = new NativeColumnFamilyConfig
        {
            WriteBufferSize = (nuint)config.WriteBufferSize,
            LevelSizeRatio = (nuint)config.LevelSizeRatio,
            MinLevels = config.MinLevels,
            DividingLevelOffset = config.DividingLevelOffset,
            KlogValueThreshold = (nuint)config.KlogValueThreshold,
            CompressionAlgo = (int)config.CompressionAlgorithm,
            EnableBloomFilter = config.EnableBloomFilter ? 1 : 0,
            BloomFpr = config.BloomFpr,
            EnableBlockIndexes = config.EnableBlockIndexes ? 1 : 0,
            IndexSampleRatio = config.IndexSampleRatio,
            BlockIndexPrefixLen = config.BlockIndexPrefixLen,
            SyncMode = (int)config.SyncMode,
            SyncIntervalUs = config.SyncIntervalUs,
            SkipListMaxLevel = config.SkipListMaxLevel,
            SkipListProbability = config.SkipListProbability,
            DefaultIsolationLevel = (int)config.DefaultIsolationLevel,
            MinDiskSpace = config.MinDiskSpace,
            L1FileCountTrigger = config.L1FileCountTrigger,
            L0QueueStallThreshold = config.L0QueueStallThreshold,
            UseBtree = config.UseBtree ? 1 : 0,
            ComparatorFnCached = nint.Zero,
            ComparatorCtxCached = nint.Zero
        };

        if (!string.IsNullOrEmpty(config.ComparatorName))
        {
            var nameBytes = Encoding.UTF8.GetBytes(config.ComparatorName);
            var copyLen = Math.Min(nameBytes.Length, 63);
            for (int i = 0; i < copyLen; i++)
            {
                nativeConfig.ComparatorName[i] = nameBytes[i];
            }
            nativeConfig.ComparatorName[copyLen] = 0;
        }

        return nativeConfig;
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_handle != nint.Zero)
        {
            NativeMethods.tidesdb_close(_handle);
            _handle = nint.Zero;
        }

        if (_dbPathPtr != nint.Zero)
        {
            Marshal.FreeHGlobal(_dbPathPtr);
            _dbPathPtr = nint.Zero;
        }
    }
}
