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
/// Exception thrown by TidesDB operations.
/// </summary>
public class TidesDBException : Exception
{
    /// <summary>
    /// The error code returned by the native library.
    /// </summary>
    public ErrorCode ErrorCode { get; }

    public TidesDBException(ErrorCode errorCode, string? context = null)
        : base(FormatMessage(errorCode, context))
    {
        ErrorCode = errorCode;
    }

    public TidesDBException(int errorCode, string? context = null)
        : this((ErrorCode)errorCode, context)
    {
    }

    private static string FormatMessage(ErrorCode errorCode, string? context)
    {
        var message = errorCode switch
        {
            ErrorCode.Success => "success",
            ErrorCode.Memory => "memory allocation failed",
            ErrorCode.InvalidArgs => "invalid arguments",
            ErrorCode.NotFound => "not found",
            ErrorCode.Io => "I/O error",
            ErrorCode.Corruption => "data corruption",
            ErrorCode.Exists => "already exists",
            ErrorCode.Conflict => "transaction conflict",
            ErrorCode.TooLarge => "key or value too large",
            ErrorCode.MemoryLimit => "memory limit exceeded",
            ErrorCode.InvalidDb => "invalid database handle",
            ErrorCode.Locked => "database is locked",
            _ => "unknown error"
        };

        return context is not null
            ? $"{context}: {message} (code: {(int)errorCode})"
            : $"{message} (code: {(int)errorCode})";
    }

    internal static void ThrowIfError(int result, string? context = null)
    {
        if (result != 0)
        {
            throw new TidesDBException(result, context);
        }
    }
}
