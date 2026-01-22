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
/// Compression algorithms supported by TidesDB.
/// </summary>
public enum CompressionAlgorithm
{
    None = 0,
    Snappy = 1,
    Lz4 = 2,
    Zstd = 3,
    Lz4Fast = 4
}

/// <summary>
/// Sync modes for durability.
/// </summary>
public enum SyncMode
{
    None = 0,
    Full = 1,
    Interval = 2
}

/// <summary>
/// Logging levels.
/// </summary>
public enum LogLevel
{
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    Fatal = 4,
    None = 99
}

/// <summary>
/// Transaction isolation levels.
/// </summary>
public enum IsolationLevel
{
    ReadUncommitted = 0,
    ReadCommitted = 1,
    RepeatableRead = 2,
    Snapshot = 3,
    Serializable = 4
}

/// <summary>
/// TidesDB error codes.
/// </summary>
public enum ErrorCode
{
    Success = 0,
    Memory = -1,
    InvalidArgs = -2,
    NotFound = -3,
    Io = -4,
    Corruption = -5,
    Exists = -6,
    Conflict = -7,
    TooLarge = -8,
    MemoryLimit = -9,
    InvalidDb = -10,
    Unknown = -11,
    Locked = -12
}
