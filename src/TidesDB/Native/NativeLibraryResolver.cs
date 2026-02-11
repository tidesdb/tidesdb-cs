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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace TidesDB.Native;

/// <summary>
/// Handles native library resolution for cross-platform support.
/// </summary>
internal static class NativeLibraryResolver
{
    private const string LibraryName = "libtidesdb";
    private static bool _initialized;
    private static readonly object _lock = new();
    private static readonly bool _enableDebugLogging = 
        Environment.GetEnvironmentVariable("TIDESDB_DEBUG") == "1" ||
        Environment.GetEnvironmentVariable("CI") == "true";

    private static void DebugLog(string message)
    {
        if (_enableDebugLogging)
        {
            Console.WriteLine($"[TidesDB.NativeLibraryResolver] {message}");
        }
    }

    /// <summary>
    /// Module initializer that runs when the assembly is first loaded,
    /// before any type in the assembly is accessed. This ensures the
    /// native library resolver is registered before P/Invoke stubs are JIT-compiled.
    /// </summary>
#pragma warning disable CA2255 // The 'ModuleInitializer' attribute should not be used in libraries
    [ModuleInitializer]
    internal static void Initialize()
#pragma warning restore CA2255
    {
        lock (_lock)
        {
            if (_initialized) return;
            
            try
            {
                DebugLog("Initializing native library resolver...");
                var assembly = Assembly.GetExecutingAssembly();
                DebugLog($"Assembly: {assembly.FullName}");
                DebugLog($"Assembly Location: {assembly.Location}");
                DebugLog($"AppContext.BaseDirectory: {AppContext.BaseDirectory}");
                DebugLog($"Environment.CurrentDirectory: {Environment.CurrentDirectory}");
                DebugLog($"OS: {Environment.OSVersion}");
                DebugLog($"Is Windows: {OperatingSystem.IsWindows()}");
                
                NativeLibrary.SetDllImportResolver(assembly, DllImportResolver);
                _initialized = true;
                DebugLog("Native library resolver initialized successfully.");
            }
            catch (Exception ex)
            {
                DebugLog($"Failed to initialize resolver: {ex.Message}");
                // Silently ignore if resolver registration fails
                // The default resolution will be used instead
                _initialized = true;
            }
        }
    }

    private static nint DllImportResolver(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        try
        {
            DebugLog($"DllImportResolver called for: {libraryName}");
            
            if (libraryName != LibraryName)
            {
                DebugLog($"Skipping - not our library (expected: {LibraryName})");
                return nint.Zero;
            }

            // Try to load from various locations
            nint handle;

            // First, try the default resolution
            DebugLog("Trying default resolution...");
            if (NativeLibrary.TryLoad(libraryName, assembly, searchPath, out handle))
            {
                DebugLog("Default resolution succeeded!");
                return handle;
            }
            DebugLog("Default resolution failed.");

            // Get the directory where the assembly is located
            var assemblyLocation = assembly.Location;
            var assemblyDir = string.IsNullOrEmpty(assemblyLocation) 
                ? AppContext.BaseDirectory 
                : Path.GetDirectoryName(assemblyLocation) ?? AppContext.BaseDirectory;

            DebugLog($"Assembly directory: {assemblyDir}");

            // Platform-specific library names to try
            var libraryNames = GetPlatformLibraryNames();
            DebugLog($"Library names to try: {string.Join(", ", libraryNames)}");

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

            DebugLog($"Search paths: {string.Join(", ", searchPaths)}");

            // Try each combination of path and library name
            foreach (var path in searchPaths.Distinct())
            {
                if (string.IsNullOrEmpty(path))
                {
                    DebugLog($"Skipping empty path");
                    continue;
                }
                
                if (!Directory.Exists(path))
                {
                    DebugLog($"Path does not exist: {path}");
                    continue;
                }

                DebugLog($"Searching in: {path}");
                foreach (var libName in libraryNames)
                {
                    var fullPath = Path.Combine(path, libName);
                    var exists = File.Exists(fullPath);
                    DebugLog($"  Checking: {fullPath} - Exists: {exists}");
                    
                    if (exists)
                    {
                        DebugLog($"  Attempting to load: {fullPath}");
                        if (NativeLibrary.TryLoad(fullPath, out handle))
                        {
                            DebugLog($"  SUCCESS: Loaded {fullPath}");
                            return handle;
                        }
                        DebugLog($"  FAILED to load: {fullPath}");
                    }
                }
            }

            // Last resort: try loading by name only (let the OS search)
            DebugLog("Trying last resort - loading by name only...");
            foreach (var libName in libraryNames)
            {
                DebugLog($"  Trying: {libName}");
                if (NativeLibrary.TryLoad(libName, out handle))
                {
                    DebugLog($"  SUCCESS: Loaded {libName}");
                    return handle;
                }
            }

            // Return zero to let the default resolver handle it (which will throw)
            DebugLog("All attempts failed. Returning zero for default resolution.");
            return nint.Zero;
        }
        catch (Exception ex)
        {
            DebugLog($"Exception in DllImportResolver: {ex.Message}");
            // If anything fails, return zero to let the default resolver handle it
            return nint.Zero;
        }
    }

    private static string[] GetPlatformLibraryNames()
    {
        if (OperatingSystem.IsWindows())
        {
            return new[]
            {
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
