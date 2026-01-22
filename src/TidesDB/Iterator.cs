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
/// Iterator for traversing key-value pairs in a column family.
/// </summary>
public sealed class Iterator : IDisposable
{
    private nint _handle;
    private bool _disposed;

    internal Iterator(nint handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Positions the iterator at the first key.
    /// </summary>
    public void SeekToFirst()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_iter_seek_to_first(_handle);
        TidesDBException.ThrowIfError(result, "failed to seek to first");
    }

    /// <summary>
    /// Positions the iterator at the last key.
    /// </summary>
    public void SeekToLast()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_iter_seek_to_last(_handle);
        TidesDBException.ThrowIfError(result, "failed to seek to last");
    }

    /// <summary>
    /// Positions the iterator at the first key >= target key.
    /// </summary>
    public void Seek(ReadOnlySpan<byte> key)
    {
        ThrowIfDisposed();
        int result;
        unsafe
        {
            fixed (byte* keyPtr = key)
            {
                result = NativeMethods.tidesdb_iter_seek(_handle, keyPtr, (nuint)key.Length);
            }
        }
        TidesDBException.ThrowIfError(result, "failed to seek");
    }

    /// <summary>
    /// Positions the iterator at the last key <= target key.
    /// </summary>
    public void SeekForPrev(ReadOnlySpan<byte> key)
    {
        ThrowIfDisposed();
        int result;
        unsafe
        {
            fixed (byte* keyPtr = key)
            {
                result = NativeMethods.tidesdb_iter_seek_for_prev(_handle, keyPtr, (nuint)key.Length);
            }
        }
        TidesDBException.ThrowIfError(result, "failed to seek for prev");
    }

    /// <summary>
    /// Returns true if the iterator is positioned at a valid entry.
    /// </summary>
    public bool Valid()
    {
        ThrowIfDisposed();
        return NativeMethods.tidesdb_iter_valid(_handle) != 0;
    }

    /// <summary>
    /// Moves the iterator to the next entry.
    /// </summary>
    public void Next()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_iter_next(_handle);
        if (result != 0 && result != (int)ErrorCode.NotFound)
        {
            TidesDBException.ThrowIfError(result, "failed to move to next");
        }
    }

    /// <summary>
    /// Moves the iterator to the previous entry.
    /// </summary>
    public void Prev()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_iter_prev(_handle);
        if (result != 0 && result != (int)ErrorCode.NotFound)
        {
            TidesDBException.ThrowIfError(result, "failed to move to prev");
        }
    }

    /// <summary>
    /// Gets the current key.
    /// </summary>
    public byte[] Key()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_iter_key(_handle, out var keyPtr, out var keySize);
        TidesDBException.ThrowIfError(result, "failed to get key");

        var key = new byte[(int)keySize];
        Marshal.Copy(keyPtr, key, 0, (int)keySize);
        return key;
    }

    /// <summary>
    /// Gets the current value.
    /// </summary>
    public byte[] Value()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_iter_value(_handle, out var valuePtr, out var valueSize);
        TidesDBException.ThrowIfError(result, "failed to get value");

        var value = new byte[(int)valueSize];
        Marshal.Copy(valuePtr, value, 0, (int)valueSize);
        return value;
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
            NativeMethods.tidesdb_iter_free(_handle);
            _handle = nint.Zero;
        }
    }
}
