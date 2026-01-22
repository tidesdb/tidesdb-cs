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
/// Exception thrown when a TidesDB operation fails.
/// </summary>
public class TidesDBException : Exception
{
    /// <summary>
    /// Gets the error code returned by TidesDB.
    /// </summary>
    public ErrorCode Code { get; }

    /// <summary>
    /// Gets the context of the operation that failed.
    /// </summary>
    public string? Context { get; }

    /// <summary>
    /// Creates a new TidesDBException.
    /// </summary>
    public TidesDBException(ErrorCode code, string? context = null)
        : base(FormatMessage(code, context))
    {
        Code = code;
        Context = context;
    }

    /// <summary>
    /// Creates a new TidesDBException with an inner exception.
    /// </summary>
    public TidesDBException(ErrorCode code, string? context, Exception innerException)
        : base(FormatMessage(code, context), innerException)
    {
        Code = code;
        Context = context;
    }

    private static string FormatMessage(ErrorCode code, string? context)
    {
        var errorMsg = code switch
        {
            ErrorCode.Success => "success",
            ErrorCode.Memory => "memory allocation failed",
            ErrorCode.InvalidArgs => "invalid arguments",
            ErrorCode.NotFound => "not found",
            ErrorCode.IO => "I/O error",
            ErrorCode.Corruption => "data corruption",
            ErrorCode.Exists => "already exists",
            ErrorCode.Conflict => "transaction conflict",
            ErrorCode.TooLarge => "key or value too large",
            ErrorCode.MemoryLimit => "memory limit exceeded",
            ErrorCode.InvalidDb => "invalid database handle",
            ErrorCode.Locked => "database is locked",
            _ => "unknown error"
        };

        return string.IsNullOrEmpty(context)
            ? $"{errorMsg} (code: {(int)code})"
            : $"{context}: {errorMsg} (code: {(int)code})";
    }

    /// <summary>
    /// Checks the result code and throws if it indicates an error.
    /// </summary>
    internal static void CheckResult(int result, string? context = null)
    {
        if (result != Native.TDB_SUCCESS)
        {
            throw new TidesDBException((ErrorCode)result, context);
        }
    }
}
