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
/// Provides bidirectional iteration over key-value pairs in a column family.
/// </summary>
public class Iterator : IDisposable
{
    private IntPtr _handle;
    private bool _disposed;

    internal Iterator(IntPtr handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Positions the iterator at the first key.
    /// </summary>
    public void SeekToFirst()
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_iter_seek_to_first(_handle);
        TidesDBException.CheckResult(result, "failed to seek to first");
    }

    /// <summary>
    /// Positions the iterator at the last key.
    /// </summary>
    public void SeekToLast()
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_iter_seek_to_last(_handle);
        TidesDBException.CheckResult(result, "failed to seek to last");
    }

    /// <summary>
    /// Positions the iterator at the first key >= target key.
    /// </summary>
    public void Seek(byte[] key)
    {
        ThrowIfDisposed();
        unsafe
        {
            fixed (byte* keyPtr = key)
            {
                var result = Native.tidesdb_iter_seek(_handle, (IntPtr)keyPtr, (nuint)key.Length);
                TidesDBException.CheckResult(result, "failed to seek");
            }
        }
    }

    /// <summary>
    /// Positions the iterator at the last key <= target key.
    /// </summary>
    public void SeekForPrev(byte[] key)
    {
        ThrowIfDisposed();
        unsafe
        {
            fixed (byte* keyPtr = key)
            {
                var result = Native.tidesdb_iter_seek_for_prev(_handle, (IntPtr)keyPtr, (nuint)key.Length);
                TidesDBException.CheckResult(result, "failed to seek for prev");
            }
        }
    }

    /// <summary>
    /// Returns true if the iterator is positioned at a valid entry.
    /// </summary>
    public bool IsValid()
    {
        ThrowIfDisposed();
        return Native.tidesdb_iter_valid(_handle) != 0;
    }

    /// <summary>
    /// Moves the iterator to the next entry.
    /// Does not throw if already at end -- use IsValid() to check.
    /// </summary>
    public void Next()
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_iter_next(_handle);
        if (result != Native.TDB_SUCCESS && result != Native.TDB_ERR_NOT_FOUND)
        {
            throw new TidesDBException((ErrorCode)result, "failed to move to next");
        }
    }

    /// <summary>
    /// Moves the iterator to the previous entry.
    /// Does not throw if already at beginning -- use IsValid() to check.
    /// </summary>
    public void Prev()
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_iter_prev(_handle);
        if (result != Native.TDB_SUCCESS && result != Native.TDB_ERR_NOT_FOUND)
        {
            throw new TidesDBException((ErrorCode)result, "failed to move to prev");
        }
    }

    /// <summary>
    /// Gets the current key.
    /// </summary>
    public byte[] Key()
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_iter_key(_handle, out var keyPtr, out var keySize);
        TidesDBException.CheckResult(result, "failed to get key");

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
        var result = Native.tidesdb_iter_value(_handle, out var valuePtr, out var valueSize);
        TidesDBException.CheckResult(result, "failed to get value");

        var value = new byte[(int)valueSize];
        Marshal.Copy(valuePtr, value, 0, (int)valueSize);
        return value;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(Iterator));
        }
    }

    /// <summary>
    /// Releases the iterator resources.
    /// </summary>
    public void Dispose()
    {
        if (!_disposed && _handle != IntPtr.Zero)
        {
            Native.tidesdb_iter_free(_handle);
            _handle = IntPtr.Zero;
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }

    ~Iterator()
    {
        Dispose();
    }
}
