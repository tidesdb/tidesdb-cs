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

namespace TidesDB.Native;

internal static partial class NativeMethods
{
    private const string LibraryName = "tidesdb";

    // Database operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_open")]
    internal static partial int tidesdb_open(ref NativeConfig config, out nint db);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_close")]
    internal static partial int tidesdb_close(nint db);

    // Column family operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_create_column_family", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int tidesdb_create_column_family(nint db, string name, ref NativeColumnFamilyConfig config);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_drop_column_family", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int tidesdb_drop_column_family(nint db, string name);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_get_column_family", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial nint tidesdb_get_column_family(nint db, string name);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_list_column_families")]
    internal static partial int tidesdb_list_column_families(nint db, out nint names, out int count);

    // Transaction operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_begin")]
    internal static partial int tidesdb_txn_begin(nint db, out nint txn);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_begin_with_isolation")]
    internal static partial int tidesdb_txn_begin_with_isolation(nint db, int isolation, out nint txn);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_put")]
    internal static unsafe partial int tidesdb_txn_put(nint txn, nint cf, byte* key, nuint keySize, byte* value, nuint valueSize, long ttl);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_get")]
    internal static unsafe partial int tidesdb_txn_get(nint txn, nint cf, byte* key, nuint keySize, out nint value, out nuint valueSize);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_delete")]
    internal static unsafe partial int tidesdb_txn_delete(nint txn, nint cf, byte* key, nuint keySize);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_commit")]
    internal static partial int tidesdb_txn_commit(nint txn);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_rollback")]
    internal static partial int tidesdb_txn_rollback(nint txn);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_free")]
    internal static partial void tidesdb_txn_free(nint txn);

    // Savepoint operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_savepoint", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int tidesdb_txn_savepoint(nint txn, string name);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_rollback_to_savepoint", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int tidesdb_txn_rollback_to_savepoint(nint txn, string name);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_release_savepoint", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int tidesdb_txn_release_savepoint(nint txn, string name);

    // Iterator operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_new")]
    internal static partial int tidesdb_iter_new(nint txn, nint cf, out nint iter);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_seek")]
    internal static unsafe partial int tidesdb_iter_seek(nint iter, byte* key, nuint keySize);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_seek_for_prev")]
    internal static unsafe partial int tidesdb_iter_seek_for_prev(nint iter, byte* key, nuint keySize);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_seek_to_first")]
    internal static partial int tidesdb_iter_seek_to_first(nint iter);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_seek_to_last")]
    internal static partial int tidesdb_iter_seek_to_last(nint iter);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_next")]
    internal static partial int tidesdb_iter_next(nint iter);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_prev")]
    internal static partial int tidesdb_iter_prev(nint iter);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_valid")]
    internal static partial int tidesdb_iter_valid(nint iter);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_key")]
    internal static partial int tidesdb_iter_key(nint iter, out nint key, out nuint keySize);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_value")]
    internal static partial int tidesdb_iter_value(nint iter, out nint value, out nuint valueSize);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_iter_free")]
    internal static partial void tidesdb_iter_free(nint iter);

    // Transaction reset
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_txn_reset")]
    internal static partial int tidesdb_txn_reset(nint txn, int isolation);

    // Column family clone/rename operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_clone_column_family", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int tidesdb_clone_column_family(nint db, string sourceName, string destName);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_rename_column_family", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int tidesdb_rename_column_family(nint db, string oldName, string newName);

    // Backup operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_backup", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int tidesdb_backup(nint db, string dir);

    // Maintenance operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_compact")]
    internal static partial int tidesdb_compact(nint cf);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_flush_memtable")]
    internal static partial int tidesdb_flush_memtable(nint cf);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_is_flushing")]
    internal static partial int tidesdb_is_flushing(nint cf);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_is_compacting")]
    internal static partial int tidesdb_is_compacting(nint cf);

    // Statistics operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_get_stats")]
    internal static partial int tidesdb_get_stats(nint cf, out nint stats);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_free_stats")]
    internal static partial void tidesdb_free_stats(nint stats);

    [LibraryImport(LibraryName, EntryPoint = "tidesdb_get_cache_stats")]
    internal static partial int tidesdb_get_cache_stats(nint db, ref NativeCacheStats stats);

    // Configuration operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_default_column_family_config")]
    internal static partial NativeColumnFamilyConfig tidesdb_default_column_family_config();

    // Comparator operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_register_comparator", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int tidesdb_register_comparator(nint db, string name, nint fn, string? ctxStr, nint ctx);

    // Memory operations
    [LibraryImport(LibraryName, EntryPoint = "tidesdb_free")]
    internal static partial void tidesdb_free(nint ptr);
}
