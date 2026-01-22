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
/// Represents a TidesDB transaction for ACID operations.
/// </summary>
public class Transaction : IDisposable
{
    private IntPtr _handle;
    private bool _disposed;

    internal Transaction(IntPtr handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Puts a key-value pair into the specified column family.
    /// </summary>
    /// <param name="cf">The column family.</param>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="ttl">TTL as Unix timestamp (seconds since epoch), or -1 for no expiration.</param>
    public void Put(ColumnFamily cf, byte[] key, byte[] value, long ttl = -1)
    {
        ThrowIfDisposed();
        unsafe
        {
            fixed (byte* keyPtr = key)
            fixed (byte* valuePtr = value)
            {
                var result = Native.tidesdb_txn_put(
                    _handle, cf.Handle,
                    (IntPtr)keyPtr, (nuint)key.Length,
                    (IntPtr)valuePtr, (nuint)value.Length,
                    ttl);
                TidesDBException.CheckResult(result, "failed to put key-value pair");
            }
        }
    }

    /// <summary>
    /// Gets a value from the specified column family.
    /// </summary>
    /// <param name="cf">The column family.</param>
    /// <param name="key">The key.</param>
    /// <returns>The value.</returns>
    public byte[] Get(ColumnFamily cf, byte[] key)
    {
        ThrowIfDisposed();
        unsafe
        {
            fixed (byte* keyPtr = key)
            {
                var result = Native.tidesdb_txn_get(
                    _handle, cf.Handle,
                    (IntPtr)keyPtr, (nuint)key.Length,
                    out var valuePtr, out var valueSize);
                TidesDBException.CheckResult(result, "failed to get value");

                var value = new byte[(int)valueSize];
                Marshal.Copy(valuePtr, value, 0, (int)valueSize);
                Native.Free(valuePtr);
                return value;
            }
        }
    }

    /// <summary>
    /// Deletes a key from the specified column family.
    /// </summary>
    /// <param name="cf">The column family.</param>
    /// <param name="key">The key.</param>
    public void Delete(ColumnFamily cf, byte[] key)
    {
        ThrowIfDisposed();
        unsafe
        {
            fixed (byte* keyPtr = key)
            {
                var result = Native.tidesdb_txn_delete(
                    _handle, cf.Handle,
                    (IntPtr)keyPtr, (nuint)key.Length);
                TidesDBException.CheckResult(result, "failed to delete key");
            }
        }
    }

    /// <summary>
    /// Commits the transaction atomically.
    /// </summary>
    public void Commit()
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_txn_commit(_handle);
        TidesDBException.CheckResult(result, "failed to commit transaction");
    }

    /// <summary>
    /// Rolls back the transaction.
    /// </summary>
    public void Rollback()
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_txn_rollback(_handle);
        TidesDBException.CheckResult(result, "failed to rollback transaction");
    }

    /// <summary>
    /// Creates a savepoint within the transaction.
    /// </summary>
    /// <param name="name">The savepoint name.</param>
    public void Savepoint(string name)
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_txn_savepoint(_handle, name);
        TidesDBException.CheckResult(result, "failed to create savepoint");
    }

    /// <summary>
    /// Rolls back to a savepoint, discarding all operations after it.
    /// </summary>
    /// <param name="name">The savepoint name.</param>
    public void RollbackToSavepoint(string name)
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_txn_rollback_to_savepoint(_handle, name);
        TidesDBException.CheckResult(result, "failed to rollback to savepoint");
    }

    /// <summary>
    /// Releases a savepoint without rolling back.
    /// </summary>
    /// <param name="name">The savepoint name.</param>
    public void ReleaseSavepoint(string name)
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_txn_release_savepoint(_handle, name);
        TidesDBException.CheckResult(result, "failed to release savepoint");
    }

    /// <summary>
    /// Creates a new iterator for the specified column family.
    /// </summary>
    /// <param name="cf">The column family.</param>
    /// <returns>A new iterator.</returns>
    public Iterator NewIterator(ColumnFamily cf)
    {
        ThrowIfDisposed();
        var result = Native.tidesdb_iter_new(_handle, cf.Handle, out var iterPtr);
        TidesDBException.CheckResult(result, "failed to create iterator");
        return new Iterator(iterPtr);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(Transaction));
        }
    }

    /// <summary>
    /// Releases the transaction resources.
    /// </summary>
    public void Dispose()
    {
        if (!_disposed && _handle != IntPtr.Zero)
        {
            Native.tidesdb_txn_free(_handle);
            _handle = IntPtr.Zero;
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }

    ~Transaction()
    {
        Dispose();
    }
}
