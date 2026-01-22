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
using System.Reflection;

namespace TidesDB;

/// <summary>
/// P/Invoke bindings for the TidesDB C library.
/// </summary>
internal static class Native
{
    private const string LibraryName = "tidesdb";

    static Native()
    {
        // Register a custom DLL resolver to help find the native library
        NativeLibrary.SetDllImportResolver(typeof(Native).Assembly, DllImportResolver);
    }

    private static IntPtr DllImportResolver(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (libraryName != LibraryName)
            return IntPtr.Zero;

        IntPtr handle = IntPtr.Zero;

        // On Windows, try MinGW naming first (libtidesdb.dll), then MSVC naming (tidesdb.dll)
        if (OperatingSystem.IsWindows())
        {
            // Try libtidesdb.dll (MinGW style) - this is what MSYS2 builds produce
            if (NativeLibrary.TryLoad("libtidesdb.dll", assembly, searchPath, out handle))
                return handle;
            // Try without assembly context to search PATH
            if (NativeLibrary.TryLoad("libtidesdb.dll", out handle))
                return handle;
            // Try libtidesdb without extension
            if (NativeLibrary.TryLoad("libtidesdb", assembly, searchPath, out handle))
                return handle;
            if (NativeLibrary.TryLoad("libtidesdb", out handle))
                return handle;
            // Try tidesdb.dll (MSVC style)
            if (NativeLibrary.TryLoad("tidesdb.dll", assembly, searchPath, out handle))
                return handle;
            if (NativeLibrary.TryLoad("tidesdb.dll", out handle))
                return handle;
            // Try default name
            if (NativeLibrary.TryLoad(libraryName, assembly, searchPath, out handle))
                return handle;
        }
        else if (OperatingSystem.IsMacOS())
        {
            if (NativeLibrary.TryLoad("libtidesdb.dylib", assembly, searchPath, out handle))
                return handle;
            if (NativeLibrary.TryLoad("libtidesdb.dylib", out handle))
                return handle;
            if (NativeLibrary.TryLoad(libraryName, assembly, searchPath, out handle))
                return handle;
        }
        else
        {
            // Linux
            if (NativeLibrary.TryLoad("libtidesdb.so", assembly, searchPath, out handle))
                return handle;
            if (NativeLibrary.TryLoad("libtidesdb.so", out handle))
                return handle;
            if (NativeLibrary.TryLoad(libraryName, assembly, searchPath, out handle))
                return handle;
        }

        return IntPtr.Zero;
    }

    // Error codes
    public const int TDB_SUCCESS = 0;
    public const int TDB_ERR_MEMORY = -1;
    public const int TDB_ERR_INVALID_ARGS = -2;
    public const int TDB_ERR_NOT_FOUND = -3;
    public const int TDB_ERR_IO = -4;
    public const int TDB_ERR_CORRUPTION = -5;
    public const int TDB_ERR_EXISTS = -6;
    public const int TDB_ERR_CONFLICT = -7;
    public const int TDB_ERR_TOO_LARGE = -8;
    public const int TDB_ERR_MEMORY_LIMIT = -9;
    public const int TDB_ERR_INVALID_DB = -10;
    public const int TDB_ERR_UNKNOWN = -11;
    public const int TDB_ERR_LOCKED = -12;

    // Configuration limits
    public const int TDB_MAX_COMPARATOR_NAME = 64;
    public const int TDB_MAX_COMPARATOR_CTX = 256;

    // Database operations
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_open(ref tidesdb_config_t config, out IntPtr db);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_close(IntPtr db);

    // Default config
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern tidesdb_column_family_config_t tidesdb_default_column_family_config();

    // Column family operations
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_create_column_family(IntPtr db, 
        [MarshalAs(UnmanagedType.LPStr)] string name, 
        ref tidesdb_column_family_config_t config);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_drop_column_family(IntPtr db, 
        [MarshalAs(UnmanagedType.LPStr)] string name);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern IntPtr tidesdb_get_column_family(IntPtr db, 
        [MarshalAs(UnmanagedType.LPStr)] string name);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_list_column_families(IntPtr db, out IntPtr names, out int count);

    // Transaction operations
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_begin(IntPtr db, out IntPtr txn);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_begin_with_isolation(IntPtr db, int isolation, out IntPtr txn);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_put(IntPtr txn, IntPtr cf, 
        IntPtr key, nuint keySize, 
        IntPtr value, nuint valueSize, 
        long ttl);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_get(IntPtr txn, IntPtr cf, 
        IntPtr key, nuint keySize, 
        out IntPtr value, out nuint valueSize);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_delete(IntPtr txn, IntPtr cf, 
        IntPtr key, nuint keySize);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_commit(IntPtr txn);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_rollback(IntPtr txn);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern void tidesdb_txn_free(IntPtr txn);

    // Savepoint operations
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_savepoint(IntPtr txn, 
        [MarshalAs(UnmanagedType.LPStr)] string name);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_rollback_to_savepoint(IntPtr txn, 
        [MarshalAs(UnmanagedType.LPStr)] string name);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_txn_release_savepoint(IntPtr txn, 
        [MarshalAs(UnmanagedType.LPStr)] string name);

    // Iterator operations
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_new(IntPtr txn, IntPtr cf, out IntPtr iter);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_seek(IntPtr iter, IntPtr key, nuint keySize);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_seek_for_prev(IntPtr iter, IntPtr key, nuint keySize);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_seek_to_first(IntPtr iter);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_seek_to_last(IntPtr iter);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_next(IntPtr iter);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_prev(IntPtr iter);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_valid(IntPtr iter);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_key(IntPtr iter, out IntPtr key, out nuint keySize);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_iter_value(IntPtr iter, out IntPtr value, out nuint valueSize);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern void tidesdb_iter_free(IntPtr iter);

    // Maintenance operations
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_compact(IntPtr cf);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_flush_memtable(IntPtr cf);

    // Statistics operations
    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_get_stats(IntPtr cf, out IntPtr stats);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern void tidesdb_free_stats(IntPtr stats);

    [DllImport(LibraryName, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    public static extern int tidesdb_get_cache_stats(IntPtr db, ref tidesdb_cache_stats_t stats);

    // Free function for memory allocated by native C code using malloc
    // TidesDB allocates memory with malloc() and expects the caller to free it with free()
    // NativeMemory.Free is a thin wrapper over the C free() API (per Microsoft docs)
    // This works cross-platform and uses the same C runtime as the native library
    public static void Free(IntPtr ptr)
    {
        if (ptr != IntPtr.Zero)
        {
            unsafe
            {
                NativeMemory.Free((void*)ptr);
            }
        }
    }

    // Structs
    [StructLayout(LayoutKind.Sequential)]
    public struct tidesdb_config_t
    {
        public IntPtr db_path;
        public int num_flush_threads;
        public int num_compaction_threads;
        public int log_level;
        private int _padding; // Padding to align block_cache_size to 8-byte boundary
        public nuint block_cache_size;
        public nuint max_open_sstables;
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    public unsafe struct tidesdb_column_family_config_t
    {
        public nuint write_buffer_size;
        public nuint level_size_ratio;
        public int min_levels;
        public int dividing_level_offset;
        public nuint klog_value_threshold;
        public int compression_algo;
        public int enable_bloom_filter;
        public double bloom_fpr;
        public int enable_block_indexes;
        public int index_sample_ratio;
        public int block_index_prefix_len;
        public int sync_mode;
        public ulong sync_interval_us;
        public fixed byte comparator_name[TDB_MAX_COMPARATOR_NAME];
        public fixed byte comparator_ctx_str[TDB_MAX_COMPARATOR_CTX];
        public IntPtr comparator_fn_cached;
        public IntPtr comparator_ctx_cached;
        public int skip_list_max_level;
        public float skip_list_probability;
        public int default_isolation_level;
        private int _padding; // Padding to align min_disk_space to 8-byte boundary
        public ulong min_disk_space;
        public int l1_file_count_trigger;
        public int l0_queue_stall_threshold;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct tidesdb_stats_t
    {
        public int num_levels;
        private int _padding; // Padding to align memtable_size to 8-byte boundary
        public nuint memtable_size;
        public IntPtr level_sizes;
        public IntPtr level_num_sstables;
        public IntPtr config;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct tidesdb_cache_stats_t
    {
        public int enabled;
        private int _padding; // Padding to align total_entries to 8-byte boundary
        public nuint total_entries;
        public nuint total_bytes;
        public ulong hits;
        public ulong misses;
        public double hit_rate;
        public nuint num_partitions;
    }
}
