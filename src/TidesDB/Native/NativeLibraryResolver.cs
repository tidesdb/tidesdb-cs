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

using System.Reflection;
using System.Runtime.InteropServices;

namespace TidesDB.Native;

/// <summary>
/// Handles native library resolution for cross-platform support.
/// </summary>
internal static class NativeLibraryResolver
{
    private const string LibraryName = "tidesdb";
    private static bool _initialized;
    private static readonly object _lock = new();

    /// <summary>
    /// Initializes the native library resolver. Must be called before any P/Invoke calls.
    /// </summary>
    internal static void Initialize()
    {
        lock (_lock)
        {
            if (_initialized) return;
            
            NativeLibrary.SetDllImportResolver(typeof(NativeLibraryResolver).Assembly, DllImportResolver);
            _initialized = true;
        }
    }

    private static nint DllImportResolver(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (libraryName != LibraryName)
        {
            return nint.Zero;
        }

        // Try to load from various locations
        nint handle;

        // First, try the default resolution
        if (NativeLibrary.TryLoad(libraryName, assembly, searchPath, out handle))
        {
            return handle;
        }

        // Get the directory where the assembly is located
        var assemblyLocation = assembly.Location;
        var assemblyDir = string.IsNullOrEmpty(assemblyLocation) 
            ? AppContext.BaseDirectory 
            : Path.GetDirectoryName(assemblyLocation) ?? AppContext.BaseDirectory;

        // Platform-specific library names to try
        var libraryNames = GetPlatformLibraryNames();

        // Search paths to try
        var searchPaths = new List<string>
        {
            assemblyDir,
            AppContext.BaseDirectory,
            Environment.CurrentDirectory
        };

        // Add platform-specific paths
        if (OperatingSystem.IsWindows())
        {
            // Add common Windows paths for MSYS2/MinGW
            var pathEnv = Environment.GetEnvironmentVariable("PATH") ?? "";
            foreach (var pathDir in pathEnv.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                if (pathDir.Contains("mingw64", StringComparison.OrdinalIgnoreCase) ||
                    pathDir.Contains("msys64", StringComparison.OrdinalIgnoreCase))
                {
                    searchPaths.Add(pathDir);
                }
            }
            
            // Common MSYS2 installation paths
            searchPaths.Add(@"C:\msys64\mingw64\bin");
            searchPaths.Add(@"D:\a\_temp\msys64\mingw64\bin");
            
            // Check RUNNER_TEMP for GitHub Actions
            var runnerTemp = Environment.GetEnvironmentVariable("RUNNER_TEMP");
            if (!string.IsNullOrEmpty(runnerTemp))
            {
                searchPaths.Add(Path.Combine(runnerTemp, "msys64", "mingw64", "bin"));
            }
        }
        else if (OperatingSystem.IsLinux())
        {
            searchPaths.Add("/usr/lib");
            searchPaths.Add("/usr/local/lib");
            searchPaths.Add("/lib");
            searchPaths.Add("/lib/x86_64-linux-gnu");
        }
        else if (OperatingSystem.IsMacOS())
        {
            searchPaths.Add("/usr/local/lib");
            searchPaths.Add("/opt/homebrew/lib");
            
            // Try to get Homebrew prefix
            var homebrewPrefix = Environment.GetEnvironmentVariable("HOMEBREW_PREFIX");
            if (!string.IsNullOrEmpty(homebrewPrefix))
            {
                searchPaths.Add(Path.Combine(homebrewPrefix, "lib"));
            }
        }

        // Try each combination of path and library name
        foreach (var path in searchPaths.Distinct())
        {
            if (string.IsNullOrEmpty(path) || !Directory.Exists(path))
                continue;

            foreach (var libName in libraryNames)
            {
                var fullPath = Path.Combine(path, libName);
                if (File.Exists(fullPath))
                {
                    if (NativeLibrary.TryLoad(fullPath, out handle))
                    {
                        return handle;
                    }
                }
            }
        }

        // Last resort: try loading by name only (let the OS search)
        foreach (var libName in libraryNames)
        {
            if (NativeLibrary.TryLoad(libName, out handle))
            {
                return handle;
            }
        }

        // Return zero to let the default resolver handle it (which will throw)
        return nint.Zero;
    }

    private static string[] GetPlatformLibraryNames()
    {
        if (OperatingSystem.IsWindows())
        {
            return new[]
            {
                "tidesdb.dll",
                "libtidesdb.dll"
            };
        }
        else if (OperatingSystem.IsMacOS())
        {
            return new[]
            {
                "libtidesdb.dylib",
                "tidesdb.dylib"
            };
        }
        else // Linux and others
        {
            return new[]
            {
                "libtidesdb.so",
                "tidesdb.so"
            };
        }
    }
}
