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

using TidesDB.Native;

namespace TidesDB;

/// <summary>
/// Process-wide TidesDB library lifecycle and capability helpers.
/// </summary>
public static class Library
{
    /// <summary>
    /// Initializes the TidesDB library with the default system allocator.
    /// Calling this is optional - <see cref="TidesDb.Open"/> lazily initializes the library on
    /// first use. Call it explicitly only if you need initialization to happen up front (for
    /// example before raising the open-file limit). Safe to call once; a second call without an
    /// intervening <see cref="Shutdown"/> is a no-op.
    /// </summary>
    /// <returns>True if the library was initialized by this call, false if it was already initialized.</returns>
    public static bool Initialize()
    {
        // tidesdb_init returns 0 on success, -1 if already initialized.
        var result = NativeMethods.tidesdb_init(nint.Zero, nint.Zero, nint.Zero, nint.Zero);
        return result == 0;
    }

    /// <summary>
    /// Finalizes the TidesDB library and resets the allocator. Call after all databases are closed.
    /// After this returns, <see cref="Initialize"/> may be called again.
    /// </summary>
    public static void Shutdown()
    {
        NativeMethods.tidesdb_finalize();
    }

    /// <summary>
    /// Raises this process's open-file ceiling toward <paramref name="desired"/> descriptors so a
    /// database can keep more SSTables open. Must be called BEFORE <see cref="TidesDb.Open"/> - the
    /// engine sizes <c>MaxOpenSstables</c> to fit the ceiling at open time. This is an explicit,
    /// opt-in operator action; TidesDB never raises the limit on its own. A failed or partial raise
    /// is non-fatal.
    /// </summary>
    /// <param name="desired">Target descriptor count; values &lt;= 0 just report the current ceiling.</param>
    /// <returns>The open-file ceiling in effect after the attempt.</returns>
    public static long RaiseOpenFileLimit(long desired)
    {
        return NativeMethods.tidesdb_raise_open_file_limit(desired);
    }

    /// <summary>
    /// Reports whether a compression backend is compiled into the loaded native library.
    /// <see cref="CompressionAlgorithm.None"/> is always available; the rest depend on the
    /// build-time <c>-DTIDESDB_WITH_*</c> options. Lets callers reject an unsupported algorithm up
    /// front instead of failing at compress/flush time.
    /// </summary>
    /// <param name="algorithm">The compression algorithm to query.</param>
    /// <returns>True if the algorithm can be used in this build.</returns>
    public static bool IsCompressionAvailable(CompressionAlgorithm algorithm)
    {
        return NativeMethods.tidesdb_compression_available((int)algorithm) == 1;
    }
}
