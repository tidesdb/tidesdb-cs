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

namespace TidesDB;

/// <summary>
/// Represents a single operation in a committed transaction batch.
/// Passed to the commit hook callback.
/// </summary>
public sealed class CommitOp
{
    /// <summary>
    /// The key data.
    /// </summary>
    public byte[] Key { get; init; } = [];

    /// <summary>
    /// The value data (null for deletes).
    /// </summary>
    public byte[]? Value { get; init; }

    /// <summary>
    /// Time-to-live for the key-value pair (0 = no expiry).
    /// </summary>
    public long Ttl { get; init; }

    /// <summary>
    /// True if this is a delete operation, false for put.
    /// </summary>
    public bool IsDelete { get; init; }
}

/// <summary>
/// Callback invoked synchronously after a transaction commits to a column family.
/// The callback receives the full batch of operations for that CF atomically.
/// </summary>
/// <param name="ops">Array of committed operations.</param>
/// <param name="commitSeq">Monotonic commit sequence number.</param>
public delegate void CommitHookHandler(CommitOp[] ops, ulong commitSeq);
