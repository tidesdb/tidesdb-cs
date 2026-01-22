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

namespace TidesDB;

/// <summary>
/// Represents a TidesDB column family - an isolated key-value store with independent configuration.
/// </summary>
public class ColumnFamily
{
    internal IntPtr Handle { get; }

    /// <summary>
    /// Gets the name of this column family.
    /// </summary>
    public string Name { get; }

    internal ColumnFamily(IntPtr handle, string name)
    {
        Handle = handle;
        Name = name;
    }

    /// <summary>
    /// Gets statistics about this column family.
    /// </summary>
    public Stats GetStats()
    {
        var result = Native.tidesdb_get_stats(Handle, out var statsPtr);
        TidesDBException.CheckResult(result, "failed to get stats");

        try
        {
            var nativeStats = Marshal.PtrToStructure<Native.tidesdb_stats_t>(statsPtr);
            
            var stats = new Stats
            {
                NumLevels = nativeStats.num_levels,
                MemtableSize = (ulong)nativeStats.memtable_size
            };

            if (nativeStats.num_levels > 0)
            {
                var levelSizes = new ulong[nativeStats.num_levels];
                var levelNumSSTables = new int[nativeStats.num_levels];

                if (nativeStats.level_sizes != IntPtr.Zero)
                {
                    for (int i = 0; i < nativeStats.num_levels; i++)
                    {
                        // size_t is nuint (platform-dependent size)
                        if (IntPtr.Size == 8)
                            levelSizes[i] = (ulong)Marshal.ReadInt64(nativeStats.level_sizes, i * sizeof(long));
                        else
                            levelSizes[i] = (ulong)Marshal.ReadInt32(nativeStats.level_sizes, i * sizeof(int));
                    }
                }

                if (nativeStats.level_num_sstables != IntPtr.Zero)
                {
                    for (int i = 0; i < nativeStats.num_levels; i++)
                    {
                        levelNumSSTables[i] = Marshal.ReadInt32(nativeStats.level_num_sstables, i * sizeof(int));
                    }
                }

                stats = stats with { LevelSizes = levelSizes, LevelNumSSTables = levelNumSSTables };
            }

            return stats;
        }
        finally
        {
            Native.tidesdb_free_stats(statsPtr);
        }
    }

    /// <summary>
    /// Manually triggers compaction for this column family.
    /// </summary>
    public void Compact()
    {
        var result = Native.tidesdb_compact(Handle);
        TidesDBException.CheckResult(result, "failed to compact column family");
    }

    /// <summary>
    /// Manually triggers memtable flush for this column family.
    /// </summary>
    public void FlushMemtable()
    {
        var result = Native.tidesdb_flush_memtable(Handle);
        TidesDBException.CheckResult(result, "failed to flush memtable");
    }
}
