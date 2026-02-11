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
using TidesDB.Native;

namespace TidesDB;

/// <summary>
/// Represents a TidesDB column family (isolated key-value store).
/// </summary>
public sealed class ColumnFamily
{
    internal readonly nint Handle;

    internal ColumnFamily(nint handle)
    {
        Handle = handle;
    }

    /// <summary>
    /// Checks if a flush operation is in progress for this column family.
    /// </summary>
    /// <returns>True if flushing is in progress.</returns>
    public bool IsFlushing()
    {
        return NativeMethods.tidesdb_is_flushing(Handle) == 1;
    }

    /// <summary>
    /// Checks if a compaction operation is in progress for this column family.
    /// </summary>
    /// <returns>True if compaction is in progress.</returns>
    public bool IsCompacting()
    {
        return NativeMethods.tidesdb_is_compacting(Handle) == 1;
    }

    /// <summary>
    /// Manually triggers compaction for this column family.
    /// </summary>
    public void Compact()
    {
        var result = NativeMethods.tidesdb_compact(Handle);
        TidesDBException.ThrowIfError(result, "failed to compact column family");
    }

    /// <summary>
    /// Manually triggers memtable flush for this column family.
    /// </summary>
    public void FlushMemtable()
    {
        var result = NativeMethods.tidesdb_flush_memtable(Handle);
        TidesDBException.ThrowIfError(result, "failed to flush memtable");
    }

    /// <summary>
    /// Gets statistics about this column family.
    /// </summary>
    public Stats GetStats()
    {
        var result = NativeMethods.tidesdb_get_stats(Handle, out var statsPtr);
        TidesDBException.ThrowIfError(result, "failed to get stats");

        try
        {
            var nativeStats = Marshal.PtrToStructure<NativeStats>(statsPtr);
            var stats = new Stats
            {
                NumLevels = nativeStats.NumLevels,
                MemtableSize = (ulong)nativeStats.MemtableSize,
                TotalKeys = nativeStats.TotalKeys,
                TotalDataSize = nativeStats.TotalDataSize,
                AvgKeySize = nativeStats.AvgKeySize,
                AvgValueSize = nativeStats.AvgValueSize,
                ReadAmp = nativeStats.ReadAmp,
                HitRate = nativeStats.HitRate,
                UseBtree = nativeStats.UseBtree != 0,
                BtreeTotalNodes = nativeStats.BtreeTotalNodes,
                BtreeMaxHeight = nativeStats.BtreeMaxHeight,
                BtreeAvgHeight = nativeStats.BtreeAvgHeight
            };

            if (nativeStats.NumLevels > 0)
            {
                var levelSizes = new ulong[nativeStats.NumLevels];
                var levelNumSstables = new int[nativeStats.NumLevels];
                var levelKeyCounts = new ulong[nativeStats.NumLevels];

                if (nativeStats.LevelSizes != nint.Zero)
                {
                    for (int i = 0; i < nativeStats.NumLevels; i++)
                    {
                        levelSizes[i] = (ulong)Marshal.ReadIntPtr(nativeStats.LevelSizes, i * nint.Size);
                    }
                }

                if (nativeStats.LevelNumSstables != nint.Zero)
                {
                    for (int i = 0; i < nativeStats.NumLevels; i++)
                    {
                        levelNumSstables[i] = Marshal.ReadInt32(nativeStats.LevelNumSstables, i * sizeof(int));
                    }
                }

                if (nativeStats.LevelKeyCounts != nint.Zero)
                {
                    for (int i = 0; i < nativeStats.NumLevels; i++)
                    {
                        levelKeyCounts[i] = (ulong)Marshal.ReadInt64(nativeStats.LevelKeyCounts, i * sizeof(ulong));
                    }
                }

                return stats with
                {
                    LevelSizes = levelSizes,
                    LevelNumSstables = levelNumSstables,
                    LevelKeyCounts = levelKeyCounts
                };
            }

            return stats;
        }
        finally
        {
            NativeMethods.tidesdb_free_stats(statsPtr);
        }
    }
}
