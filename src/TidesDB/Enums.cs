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

namespace TidesDB;

/// <summary>
/// Compression algorithms supported by TidesDB.
/// </summary>
public enum CompressionAlgorithm
{
    /// <summary>No compression.</summary>
    None = 0,
    /// <summary>Snappy compression (not available on SunOS).</summary>
    Snappy = 1,
    /// <summary>LZ4 compression (default).</summary>
    Lz4 = 2,
    /// <summary>Zstandard compression.</summary>
    Zstd = 3,
    /// <summary>LZ4 fast compression.</summary>
    Lz4Fast = 4
}

/// <summary>
/// Sync modes for durability control.
/// </summary>
public enum SyncMode
{
    /// <summary>No sync - fastest, least durable.</summary>
    None = 0,
    /// <summary>Full sync - fsync on every write.</summary>
    Full = 1,
    /// <summary>Interval sync - periodic background syncing.</summary>
    Interval = 2
}

/// <summary>
/// Logging levels for TidesDB.
/// </summary>
public enum LogLevel
{
    /// <summary>Debug level - detailed diagnostic information.</summary>
    Debug = 0,
    /// <summary>Info level - general informational messages.</summary>
    Info = 1,
    /// <summary>Warn level - warning messages.</summary>
    Warn = 2,
    /// <summary>Error level - error messages.</summary>
    Error = 3,
    /// <summary>Fatal level - critical errors.</summary>
    Fatal = 4,
    /// <summary>None - disable all logging.</summary>
    None = 99
}

/// <summary>
/// Transaction isolation levels.
/// </summary>
public enum IsolationLevel
{
    /// <summary>Read uncommitted - sees all data including uncommitted changes.</summary>
    ReadUncommitted = 0,
    /// <summary>Read committed - sees only committed data (default).</summary>
    ReadCommitted = 1,
    /// <summary>Repeatable read - consistent snapshot, phantom reads possible.</summary>
    RepeatableRead = 2,
    /// <summary>Snapshot isolation - write-write conflict detection.</summary>
    Snapshot = 3,
    /// <summary>Serializable - full read-write conflict detection (SSI).</summary>
    Serializable = 4
}

/// <summary>
/// Error codes returned by TidesDB operations.
/// </summary>
public enum ErrorCode
{
    /// <summary>Operation completed successfully.</summary>
    Success = 0,
    /// <summary>Memory allocation failed.</summary>
    Memory = -1,
    /// <summary>Invalid arguments passed to function.</summary>
    InvalidArgs = -2,
    /// <summary>Key not found.</summary>
    NotFound = -3,
    /// <summary>I/O error.</summary>
    IO = -4,
    /// <summary>Data corruption detected.</summary>
    Corruption = -5,
    /// <summary>Resource already exists.</summary>
    Exists = -6,
    /// <summary>Transaction conflict detected.</summary>
    Conflict = -7,
    /// <summary>Key or value too large.</summary>
    TooLarge = -8,
    /// <summary>Memory limit exceeded.</summary>
    MemoryLimit = -9,
    /// <summary>Invalid database handle.</summary>
    InvalidDb = -10,
    /// <summary>Unknown error.</summary>
    Unknown = -11,
    /// <summary>Database is locked.</summary>
    Locked = -12
}
