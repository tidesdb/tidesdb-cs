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
/// Represents a TidesDB transaction.
/// </summary>
public sealed class Transaction : IDisposable
{
    private nint _handle;
    private bool _disposed;

    internal Transaction(nint handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Puts a key-value pair into the transaction.
    /// </summary>
    /// <param name="cf">The column family.</param>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="ttl">Unix timestamp for expiration, or -1 for no expiration.</param>
    public void Put(ColumnFamily cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, long ttl = -1)
    {
        ThrowIfDisposed();
        int result;
        unsafe
        {
            fixed (byte* keyPtr = key)
            fixed (byte* valuePtr = value)
            {
                result = NativeMethods.tidesdb_txn_put(
                    _handle, cf.Handle,
                    keyPtr, (nuint)key.Length,
                    valuePtr, (nuint)value.Length,
                    ttl);
            }
        }
        TidesDBException.ThrowIfError(result, "failed to put key-value pair");
    }

    /// <summary>
    /// Gets a value from the transaction.
    /// </summary>
    /// <param name="cf">The column family.</param>
    /// <param name="key">The key.</param>
    /// <returns>The value, or null if not found.</returns>
    public byte[]? Get(ColumnFamily cf, ReadOnlySpan<byte> key)
    {
        ThrowIfDisposed();
        int result;
        nint valuePtr;
        nuint valueSize;

        unsafe
        {
            fixed (byte* keyPtr = key)
            {
                result = NativeMethods.tidesdb_txn_get(
                    _handle, cf.Handle,
                    keyPtr, (nuint)key.Length,
                    out valuePtr, out valueSize);
            }
        }

        if (result == (int)ErrorCode.NotFound)
        {
            return null;
        }

        TidesDBException.ThrowIfError(result, "failed to get value");

        var value = new byte[(int)valueSize];
        Marshal.Copy(valuePtr, value, 0, (int)valueSize);
        NativeMethods.tidesdb_free(valuePtr);
        return value;
    }

    /// <summary>
    /// Deletes a key-value pair from the transaction.
    /// </summary>
    /// <param name="cf">The column family.</param>
    /// <param name="key">The key.</param>
    public void Delete(ColumnFamily cf, ReadOnlySpan<byte> key)
    {
        ThrowIfDisposed();
        int result;
        unsafe
        {
            fixed (byte* keyPtr = key)
            {
                result = NativeMethods.tidesdb_txn_delete(
                    _handle, cf.Handle,
                    keyPtr, (nuint)key.Length);
            }
        }
        TidesDBException.ThrowIfError(result, "failed to delete key");
    }

    /// <summary>
    /// Commits the transaction.
    /// </summary>
    public void Commit()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_txn_commit(_handle);
        TidesDBException.ThrowIfError(result, "failed to commit transaction");
    }

    /// <summary>
    /// Rolls back the transaction.
    /// </summary>
    public void Rollback()
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_txn_rollback(_handle);
        TidesDBException.ThrowIfError(result, "failed to rollback transaction");
    }

    /// <summary>
    /// Creates a savepoint within the transaction.
    /// </summary>
    /// <param name="name">The savepoint name.</param>
    public void Savepoint(string name)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_txn_savepoint(_handle, name);
        TidesDBException.ThrowIfError(result, "failed to create savepoint");
    }

    /// <summary>
    /// Rolls back the transaction to a savepoint.
    /// </summary>
    /// <param name="name">The savepoint name.</param>
    public void RollbackToSavepoint(string name)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_txn_rollback_to_savepoint(_handle, name);
        TidesDBException.ThrowIfError(result, "failed to rollback to savepoint");
    }

    /// <summary>
    /// Releases a savepoint without rolling back.
    /// </summary>
    /// <param name="name">The savepoint name.</param>
    public void ReleaseSavepoint(string name)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_txn_release_savepoint(_handle, name);
        TidesDBException.ThrowIfError(result, "failed to release savepoint");
    }

    /// <summary>
    /// Resets a committed or aborted transaction for reuse with a new isolation level.
    /// This avoids the overhead of freeing and reallocating transaction resources.
    /// </summary>
    /// <param name="isolation">The new isolation level for the reset transaction.</param>
    public void Reset(IsolationLevel isolation)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_txn_reset(_handle, (int)isolation);
        TidesDBException.ThrowIfError(result, "failed to reset transaction");
    }

    /// <summary>
    /// Creates a new iterator for a column family within this transaction.
    /// </summary>
    /// <param name="cf">The column family.</param>
    /// <returns>A new iterator.</returns>
    public Iterator NewIterator(ColumnFamily cf)
    {
        ThrowIfDisposed();
        var result = NativeMethods.tidesdb_iter_new(_handle, cf.Handle, out var iterHandle);
        TidesDBException.ThrowIfError(result, "failed to create iterator");
        return new Iterator(iterHandle);
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
            NativeMethods.tidesdb_txn_free(_handle);
            _handle = nint.Zero;
        }
    }
}
