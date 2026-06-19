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

using System.Text;
using Xunit;

namespace TidesDB.Tests;

/// <summary>
/// Tests for FFI surface added to track db.h -- write-amplification stats, ErrorCode.Busy,
/// init/finalize, open-file limit, compression availability, background-work cancellation,
/// INI config round-trip, finish-compactions-on-close, and comparator registration.
/// </summary>
public class FfiCoverageTests : IDisposable
{
    private readonly string _testDbPath;
    private TidesDb? _db;

    public FfiCoverageTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"tidesdb_new_{Guid.NewGuid()}");
    }

    public void Dispose()
    {
        _db?.Dispose();
        if (Directory.Exists(_testDbPath))
        {
            Directory.Delete(_testDbPath, true);
        }
    }

    private TidesDb OpenDatabase()
    {
        _db = TidesDb.Open(new Config { DbPath = _testDbPath });
        return _db;
    }

    // Library-level helpers

    [Fact]
    public void Library_RaiseOpenFileLimit_ReportsCeiling()
    {
        // Querying with <= 0 just reports the current ceiling without changing it.
        var ceiling = Library.RaiseOpenFileLimit(0);
        Assert.True(ceiling > 0);
    }

    [Fact]
    public void Library_IsCompressionAvailable_NoneAlwaysTrue()
    {
        Assert.True(Library.IsCompressionAvailable(CompressionAlgorithm.None));
    }

    [Fact]
    public void Library_IsCompressionAvailable_Lz4_MatchesUsableAlgorithm()
    {
        // Whatever the build reports as available must actually be usable for a CF.
        if (Library.IsCompressionAvailable(CompressionAlgorithm.Lz4))
        {
            using var db = OpenDatabase();
            db.CreateColumnFamily("lz4_cf", new ColumnFamilyConfig
            {
                CompressionAlgorithm = CompressionAlgorithm.Lz4
            });
            Assert.NotNull(db.GetColumnFamily("lz4_cf"));
        }
    }

    [Fact]
    public void Library_Initialize_IsIdempotentAfterOpen()
    {
        // Opening a database lazily initializes the library, so a subsequent explicit
        // Initialize() is a no-op and must report "already initialized" rather than throw.
        using var db = OpenDatabase();
        Assert.False(Library.Initialize());
    }

    // ErrorCode.Busy

    [Fact]
    public void ErrorCode_Busy_HasExpectedValue()
    {
        Assert.Equal(-14, (int)ErrorCode.Busy);
        var ex = new TidesDBException(ErrorCode.Busy, "ctx");
        Assert.Equal(ErrorCode.Busy, ex.ErrorCode);
        Assert.Contains("busy", ex.Message);
    }

    // ErrorCode.Precondition (TDB_ERR_PRECONDITION = -15)

    [Fact]
    public void ErrorCode_Precondition_HasExpectedValue()
    {
        Assert.Equal(-15, (int)ErrorCode.Precondition);
        var ex = new TidesDBException(ErrorCode.Precondition, "ctx");
        Assert.Equal(ErrorCode.Precondition, ex.ErrorCode);
        Assert.Contains("precondition", ex.Message);
    }

    // finish_compactions_on_close ABI field

    [Fact]
    public void Config_FinishCompactionsOnClose_OpensAndCloses()
    {
        var db = TidesDb.Open(new Config
        {
            DbPath = _testDbPath,
            FinishCompactionsOnClose = true
        });
        db.CreateColumnFamily("cf");
        var cf = db.GetColumnFamily("cf")!;
        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, "k"u8.ToArray(), "v"u8.ToArray());
            txn.Commit();
        }
        // The struct layout fix means close reads a valid flag (no OOB read / corruption).
        db.Dispose();
    }

    // CancelBackgroundWork

    [Fact]
    public void CancelBackgroundWork_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("cf");
        var cf = db.GetColumnFamily("cf")!;
        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, "k"u8.ToArray(), "v"u8.ToArray());
            txn.Commit();
        }
        db.CancelBackgroundWork();
    }

    // Write-amplification stats (struct tail fields)

    [Fact]
    public void GetStats_WriteAmpFields_ArePopulatedAfterFlush()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("wa_cf", new ColumnFamilyConfig { WriteBufferSize = 64 * 1024 });
        var cf = db.GetColumnFamily("wa_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 500; i++)
            {
                txn.Put(cf, Encoding.UTF8.GetBytes($"key{i:D5}"), Encoding.UTF8.GetBytes($"value{i}"));
            }
            txn.Commit();
        }
        cf.FlushMemtable();

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (cf.IsFlushing() && DateTime.UtcNow < deadline) Thread.Sleep(50);

        var stats = cf.GetStats();
        // user_bytes_written counts logical committed key+value bytes; must be non-zero.
        Assert.True(stats.UserBytesWritten > 0, "expected UserBytesWritten > 0 after commits");
        Assert.True(stats.WalBytesWritten > 0, "expected WalBytesWritten > 0 in per-CF (non-unified) mode");
    }

    [Fact]
    public void GetDbStats_WriteAmpFields_AreReadable()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("cf");
        var cf = db.GetColumnFamily("cf")!;
        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, "k"u8.ToArray(), "v"u8.ToArray());
            txn.Commit();
        }

        var stats = db.GetDbStats();
        // Reading these tail fields proves the NativeDbStats layout matches the C struct
        // (a short struct would have corrupted memory on the native write - the
        // primary_epoch/seen_epoch fencing fields sit between replica_mode and the WA counters).
        Assert.True(stats.UserBytesWritten > 0);
        Assert.True(stats.UwalBytesWritten == 0, "unified WAL volume is 0 when unified mode is off");
        // Single-writer fencing epochs are local-mode quiet (no object store, no lease).
        Assert.False(stats.ReplicaMode);
        Assert.Equal(0UL, stats.PrimaryEpoch);
        Assert.Equal(0UL, stats.SeenEpoch);
    }

    // Object store mode db stats (filesystem connector)

    [Fact]
    public void GetDbStats_ObjectStoreMode_ReportsConnectorAndFencingFields()
    {
        var osRoot = Path.Combine(Path.GetTempPath(), $"tidesdb_os_{Guid.NewGuid()}");
        Directory.CreateDirectory(osRoot);
        try
        {
            using var db = TidesDb.Open(new Config
            {
                DbPath = _testDbPath,
                ObjectStoreConfig = new ObjectStoreConfig
                {
                    ConnectorType = ObjectStoreConnectorType.Filesystem,
                    FsRootDir = osRoot
                }
            });
            db.CreateColumnFamily("cf");
            var cf = db.GetColumnFamily("cf")!;
            using (var txn = db.BeginTransaction())
            {
                txn.Put(cf, "k"u8.ToArray(), "v"u8.ToArray());
                txn.Commit();
            }

            var stats = db.GetDbStats();
            // Object store mode reads the full tail of the struct -- connector name plus the
            // fencing/upload fields all decode at the correct offsets.
            Assert.True(stats.ObjectStoreEnabled);
            Assert.False(string.IsNullOrEmpty(stats.ObjectStoreConnector));
            // A fresh non-replica primary holds no acquired lease; the fencing fields decode at
            // the right offsets and the connector name (struct tail) is intact.
            Assert.False(stats.ReplicaMode);
        }
        finally
        {
            if (Directory.Exists(osRoot)) Directory.Delete(osRoot, true);
        }
    }

    // INI config round-trip

    [Fact]
    public void CfConfig_SaveAndLoadIni_RoundTrips()
    {
        var iniFile = Path.Combine(Path.GetTempPath(), $"tidesdb_cfg_{Guid.NewGuid()}.ini");
        try
        {
            var original = new ColumnFamilyConfig
            {
                WriteBufferSize = 32 * 1024 * 1024,
                CompressionAlgorithm = CompressionAlgorithm.Zstd,
                BloomFpr = 0.005,
                MinLevels = 7,
                UseBtree = true
            };

            original.SaveToIni(iniFile, "mycf");
            Assert.True(File.Exists(iniFile));

            var loaded = ColumnFamilyConfig.LoadFromIni(iniFile, "mycf");
            Assert.Equal(original.WriteBufferSize, loaded.WriteBufferSize);
            Assert.Equal(original.CompressionAlgorithm, loaded.CompressionAlgorithm);
            Assert.Equal(original.BloomFpr, loaded.BloomFpr);
            Assert.Equal(original.MinLevels, loaded.MinLevels);
            Assert.Equal(original.UseBtree, loaded.UseBtree);
        }
        finally
        {
            if (File.Exists(iniFile)) File.Delete(iniFile);
        }
    }

    [Fact]
    public void CfConfig_LoadFromIni_UsableForCreate()
    {
        var iniFile = Path.Combine(Path.GetTempPath(), $"tidesdb_cfg_{Guid.NewGuid()}.ini");
        try
        {
            new ColumnFamilyConfig { CompressionAlgorithm = CompressionAlgorithm.Lz4 }
                .SaveToIni(iniFile, "section1");
            var loaded = ColumnFamilyConfig.LoadFromIni(iniFile, "section1");

            using var db = OpenDatabase();
            db.CreateColumnFamily("from_ini", loaded);
            Assert.NotNull(db.GetColumnFamily("from_ini"));
        }
        finally
        {
            if (File.Exists(iniFile)) File.Delete(iniFile);
        }
    }

    // Comparator registration

    [Fact]
    public void RegisterBuiltInComparator_UnderCustomName_ShouldRegister()
    {
        using var db = OpenDatabase();
        db.RegisterBuiltInComparator("my_uint64", BuiltInComparator.Uint64);
        Assert.True(db.GetComparator("my_uint64"));
    }

    [Fact]
    public void RegisterBuiltInComparator_DrivesCorrectOrdering()
    {
        using var db = OpenDatabase();
        db.RegisterBuiltInComparator("reverse_alias", BuiltInComparator.ReverseMemcmp);
        db.CreateColumnFamily("rev_cf", new ColumnFamilyConfig { ComparatorName = "reverse_alias" });
        var cf = db.GetColumnFamily("rev_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, "a"u8.ToArray(), "1"u8.ToArray());
            txn.Put(cf, "b"u8.ToArray(), "2"u8.ToArray());
            txn.Put(cf, "c"u8.ToArray(), "3"u8.ToArray());
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        using var iter = readTxn.NewIterator(cf);
        iter.SeekToFirst();
        Assert.True(iter.Valid());
        // Reverse comparator => "c" sorts first.
        Assert.Equal("c", Encoding.UTF8.GetString(iter.Key()));
    }

    [Fact]
    public void RegisterComparator_ManagedDelegate_ShouldRegisterAndOrder()
    {
        using var db = OpenDatabase();
        int callCount = 0;

        // Order purely by key length, then by bytes (a total order).
        db.RegisterComparator("by_length", (k1, k2) =>
        {
            Interlocked.Increment(ref callCount);
            if (k1.Length != k2.Length) return k1.Length - k2.Length;
            return k1.SequenceCompareTo(k2);
        });

        Assert.True(db.GetComparator("by_length"));

        db.CreateColumnFamily("len_cf", new ColumnFamilyConfig { ComparatorName = "by_length" });
        var cf = db.GetColumnFamily("len_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, "ccc"u8.ToArray(), "3"u8.ToArray());
            txn.Put(cf, "a"u8.ToArray(), "1"u8.ToArray());
            txn.Put(cf, "bb"u8.ToArray(), "2"u8.ToArray());
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        using var iter = readTxn.NewIterator(cf);
        iter.SeekToFirst();

        var order = new List<string>();
        while (iter.Valid())
        {
            order.Add(Encoding.UTF8.GetString(iter.Key()));
            iter.Next();
        }

        Assert.Equal(new[] { "a", "bb", "ccc" }, order);
        Assert.True(callCount > 0, "managed comparator delegate should have been invoked");
    }

    [Fact]
    public void RegisterComparator_NullDelegate_ShouldThrow()
    {
        using var db = OpenDatabase();
        Assert.Throws<ArgumentNullException>(() => db.RegisterComparator("bad", null!));
    }

    [Fact]
    public void RegisterComparator_DuplicateName_ShouldThrow()
    {
        using var db = OpenDatabase();
        // "memcmp" is auto-registered on open; re-registering must fail with InvalidArgs.
        var ex = Assert.Throws<TidesDBException>(() =>
            db.RegisterBuiltInComparator("memcmp", BuiltInComparator.Memcmp));
        Assert.Equal(ErrorCode.InvalidArgs, ex.ErrorCode);
    }
}
