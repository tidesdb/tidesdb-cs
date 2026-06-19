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

using System.Reflection;
using System.Runtime.InteropServices;

namespace TidesDB;

/// <summary>
/// A custom key comparator. Returns a negative value if <paramref name="key1"/> sorts before
/// <paramref name="key2"/>, zero if they are equal, and a positive value if it sorts after.
/// </summary>
/// <remarks>
/// The callback is invoked from native code on the hot path of every memtable and compaction
/// comparison, potentially concurrently on background threads. It must be deterministic, total,
/// thread-safe, and must not throw (an exception is caught and treated as "equal"). Keep it fast.
/// The keys are valid only for the duration of the call - do not retain the spans.
/// </remarks>
/// <param name="key1">The first key.</param>
/// <param name="key2">The second key.</param>
/// <returns>Negative, zero, or positive per the ordering of <paramref name="key1"/> vs <paramref name="key2"/>.</returns>
public delegate int ComparatorFunction(ReadOnlySpan<byte> key1, ReadOnlySpan<byte> key2);

/// <summary>
/// The built-in comparators TidesDB ships with. Each is registered automatically on database open
/// under its conventional name (so a column family can reference it via
/// <see cref="ColumnFamilyConfig.ComparatorName"/>); this enum lets you also register one under a
/// custom name via <see cref="TidesDb.RegisterBuiltInComparator"/>.
/// </summary>
public enum BuiltInComparator
{
    /// <summary>Binary, byte-by-byte comparison (the default, registered as "memcmp").</summary>
    Memcmp,

    /// <summary>Null-terminated string comparison (registered as "lexicographic").</summary>
    Lexicographic,

    /// <summary>Unsigned 64-bit integer comparison (registered as "uint64").</summary>
    Uint64,

    /// <summary>Signed 64-bit integer comparison (registered as "int64").</summary>
    Int64,

    /// <summary>Reverse binary comparison (registered as "reverse").</summary>
    ReverseMemcmp,

    /// <summary>Case-insensitive ASCII comparison (registered as "case_insensitive").</summary>
    CaseInsensitive
}

[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate int NativeComparatorFn(nint key1, nuint key1Size, nint key2, nuint key2Size, nint ctx);

/// <summary>
/// Internal plumbing for comparator registration, the managed-to-native bridge and built-in
/// function-pointer resolution.
/// </summary>
internal static class ComparatorBridge
{
    internal static readonly NativeComparatorFn Bridge = Invoke;
    internal static readonly nint BridgePtr = Marshal.GetFunctionPointerForDelegate(Bridge);

    private static unsafe int Invoke(nint key1, nuint key1Size, nint key2, nuint key2Size, nint ctx)
    {
        try
        {
            var handle = GCHandle.FromIntPtr(ctx);
            var comparator = (ComparatorFunction)handle.Target!;
            var span1 = new ReadOnlySpan<byte>((void*)key1, (int)key1Size);
            var span2 = new ReadOnlySpan<byte>((void*)key2, (int)key2Size);
            return comparator(span1, span2);
        }
        catch
        {
            return 0;
        }
    }

    private static readonly object s_libLock = new();
    private static nint s_libHandle;

    /// <summary>
    /// Resolves the raw native function pointer for a built-in comparator export so it can be
    /// registered under a custom name with native-speed comparison (no managed callback).
    /// </summary>
    internal static nint GetBuiltInPointer(BuiltInComparator comparator)
    {
        var entryPoint = comparator switch
        {
            BuiltInComparator.Memcmp => "tidesdb_comparator_memcmp",
            BuiltInComparator.Lexicographic => "tidesdb_comparator_lexicographic",
            BuiltInComparator.Uint64 => "tidesdb_comparator_uint64",
            BuiltInComparator.Int64 => "tidesdb_comparator_int64",
            BuiltInComparator.ReverseMemcmp => "tidesdb_comparator_reverse_memcmp",
            BuiltInComparator.CaseInsensitive => "tidesdb_comparator_case_insensitive",
            _ => throw new ArgumentOutOfRangeException(nameof(comparator))
        };

        if (s_libHandle == nint.Zero)
        {
            lock (s_libLock)
            {
                if (s_libHandle == nint.Zero)
                {
                    var assembly = typeof(ComparatorBridge).Assembly;
                    // Routes through the registered DllImportResolver (NativeLibraryResolver).
                    s_libHandle = NativeLibrary.Load("libtidesdb", assembly, null);
                }
            }
        }

        return NativeLibrary.GetExport(s_libHandle, entryPoint);
    }
}
