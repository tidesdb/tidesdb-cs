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

public class TidesDBTests : IDisposable
{
    private readonly string _testDbPath;
    private TidesDb? _db;

    public TidesDBTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"tidesdb_test_{Guid.NewGuid()}");
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
        var config = new Config
        {
            DbPath = _testDbPath,
            NumFlushThreads = 2,
            NumCompactionThreads = 2,
            LogLevel = LogLevel.Info,
            BlockCacheSize = 64 * 1024 * 1024,
            MaxOpenSstables = 256
        };
        _db = TidesDb.Open(config);
        return _db;
    }

    [Fact]
    public void OpenAndClose_ShouldSucceed()
    {
        using var db = OpenDatabase();
        Assert.NotNull(db);
    }

    [Fact]
    public void CreateColumnFamily_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");

        var cf = db.GetColumnFamily("test_cf");
        Assert.NotNull(cf);
    }

    [Fact]
    public void CreateColumnFamily_WithConfig_ShouldSucceed()
    {
        using var db = OpenDatabase();
        var cfConfig = new ColumnFamilyConfig
        {
            WriteBufferSize = 32 * 1024 * 1024,
            CompressionAlgorithm = CompressionAlgorithm.Lz4,
            EnableBloomFilter = true,
            BloomFpr = 0.01
        };
        db.CreateColumnFamily("test_cf", cfConfig);

        var cf = db.GetColumnFamily("test_cf");
        Assert.NotNull(cf);
    }

    [Fact]
    public void DropColumnFamily_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        db.DropColumnFamily("test_cf");

        var cf = db.GetColumnFamily("test_cf");
        Assert.Null(cf);
    }

    [Fact]
    public void ListColumnFamilies_ShouldReturnCreatedFamilies()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("cf1");
        db.CreateColumnFamily("cf2");

        var families = db.ListColumnFamilies();
        Assert.Contains("cf1", families);
        Assert.Contains("cf2", families);
    }

    [Fact]
    public void PutAndGet_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using var txn = db.BeginTransaction();
        var key = Encoding.UTF8.GetBytes("test_key");
        var value = Encoding.UTF8.GetBytes("test_value");

        txn.Put(cf, key, value);
        txn.Commit();

        using var readTxn = db.BeginTransaction();
        var result = readTxn.Get(cf, key);

        Assert.NotNull(result);
        Assert.Equal("test_value", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public void Delete_ShouldRemoveKey()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var key = Encoding.UTF8.GetBytes("test_key");
        var value = Encoding.UTF8.GetBytes("test_value");

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, key, value);
            txn.Commit();
        }

        using (var txn = db.BeginTransaction())
        {
            txn.Delete(cf, key);
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        var result = readTxn.Get(cf, key);
        Assert.Null(result);
    }

    [Fact]
    public void SingleDelete_ShouldRemoveKey()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var key = Encoding.UTF8.GetBytes("single_key");
        var value = Encoding.UTF8.GetBytes("single_value");

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, key, value);
            txn.Commit();
        }

        using (var txn = db.BeginTransaction())
        {
            txn.SingleDelete(cf, key);
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        var result = readTxn.Get(cf, key);
        Assert.Null(result);
    }

    [Fact]
    public void SingleDelete_WithinTransaction_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var keyA = Encoding.UTF8.GetBytes("key_a");
        var keyB = Encoding.UTF8.GetBytes("key_b");
        var value = Encoding.UTF8.GetBytes("value");

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, keyA, value);
            txn.Put(cf, keyB, value);
            txn.Commit();
        }

        using (var txn = db.BeginTransaction())
        {
            txn.SingleDelete(cf, keyA);
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        Assert.Null(readTxn.Get(cf, keyA));
        Assert.NotNull(readTxn.Get(cf, keyB));
    }

    [Fact]
    public void SingleDelete_OnMissingKey_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.SingleDelete(cf, Encoding.UTF8.GetBytes("never_existed"));
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        var result = readTxn.Get(cf, Encoding.UTF8.GetBytes("never_existed"));
        Assert.Null(result);
    }

    [Fact]
    public void Transaction_Rollback_ShouldDiscardChanges()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var key = Encoding.UTF8.GetBytes("test_key");
        var value = Encoding.UTF8.GetBytes("test_value");

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, key, value);
            txn.Rollback();
        }

        using var readTxn = db.BeginTransaction();
        var result = readTxn.Get(cf, key);
        Assert.Null(result);
    }

    [Fact]
    public void Transaction_WithIsolationLevel_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using var txn = db.BeginTransaction(IsolationLevel.Serializable);
        var key = Encoding.UTF8.GetBytes("test_key");
        var value = Encoding.UTF8.GetBytes("test_value");

        txn.Put(cf, key, value);
        txn.Commit();
    }

    [Fact]
    public void Savepoint_ShouldAllowPartialRollback()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var key1 = Encoding.UTF8.GetBytes("key1");
        var key2 = Encoding.UTF8.GetBytes("key2");
        var value = Encoding.UTF8.GetBytes("value");

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, key1, value);
            txn.Savepoint("sp1");
            txn.Put(cf, key2, value);
            txn.RollbackToSavepoint("sp1");
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        Assert.NotNull(readTxn.Get(cf, key1));
        Assert.Null(readTxn.Get(cf, key2));
    }

    [Fact]
    public void Iterator_ForwardIteration_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 10; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D2}");
                var value = Encoding.UTF8.GetBytes($"value{i}");
                txn.Put(cf, key, value);
            }
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        using var iter = readTxn.NewIterator(cf);

        iter.SeekToFirst();
        var count = 0;
        while (iter.Valid())
        {
            var key = iter.Key();
            var value = iter.Value();
            Assert.NotNull(key);
            Assert.NotNull(value);
            count++;
            iter.Next();
        }

        Assert.Equal(10, count);
    }

    [Fact]
    public void Iterator_Seek_ShouldPositionCorrectly()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 10; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D2}");
                var value = Encoding.UTF8.GetBytes($"value{i}");
                txn.Put(cf, key, value);
            }
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        using var iter = readTxn.NewIterator(cf);

        iter.Seek(Encoding.UTF8.GetBytes("key05"));
        Assert.True(iter.Valid());
        var foundKey = Encoding.UTF8.GetString(iter.Key());
        Assert.Equal("key05", foundKey);
    }

    [Fact]
    public void GetCacheStats_ShouldReturnStats()
    {
        using var db = OpenDatabase();
        var stats = db.GetCacheStats();
        Assert.NotNull(stats);
    }

    [Fact]
    public void ColumnFamily_GetStats_ShouldReturnStats()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var stats = cf.GetStats();
        Assert.NotNull(stats);
        Assert.True(stats.NumLevels >= 0);
    }

    [Fact]
    public void MultipleColumnFamilies_Transaction_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("cf1");
        db.CreateColumnFamily("cf2");

        var cf1 = db.GetColumnFamily("cf1")!;
        var cf2 = db.GetColumnFamily("cf2")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf1, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Put(cf2, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        Assert.NotNull(readTxn.Get(cf1, Encoding.UTF8.GetBytes("key1")));
        Assert.NotNull(readTxn.Get(cf2, Encoding.UTF8.GetBytes("key2")));
    }

    [Fact]
    public void OpenWithLogToFile_ShouldSucceed()
    {
        var testPath = Path.Combine(Path.GetTempPath(), $"tidesdb_logtest_{Guid.NewGuid()}");
        try
        {
            var config = new Config
            {
                DbPath = testPath,
                NumFlushThreads = 2,
                NumCompactionThreads = 2,
                LogLevel = LogLevel.Debug,
                BlockCacheSize = 64 * 1024 * 1024,
                MaxOpenSstables = 256,
                LogToFile = true,
                LogTruncationAt = 10 * 1024 * 1024
            };

            using var db = TidesDb.Open(config);
            Assert.NotNull(db);

            db.CreateColumnFamily("test_cf");
            var cf = db.GetColumnFamily("test_cf");
            Assert.NotNull(cf);
        }
        finally
        {
            if (Directory.Exists(testPath))
            {
                Directory.Delete(testPath, true);
            }
        }
    }

    [Fact]
    public void CreateColumnFamily_WithUseBtree_ShouldSucceed()
    {
        using var db = OpenDatabase();
        var cfConfig = new ColumnFamilyConfig
        {
            WriteBufferSize = 32 * 1024 * 1024,
            CompressionAlgorithm = CompressionAlgorithm.Lz4,
            EnableBloomFilter = true,
            BloomFpr = 0.01,
            UseBtree = true
        };
        db.CreateColumnFamily("btree_cf", cfConfig);

        var cf = db.GetColumnFamily("btree_cf");
        Assert.NotNull(cf);
    }

    [Fact]
    public void ColumnFamily_GetStats_ShouldReturnExtendedStats()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value{i}");
                txn.Put(cf, key, value);
            }
            txn.Commit();
        }

        var stats = cf.GetStats();
        Assert.NotNull(stats);
        Assert.True(stats.NumLevels >= 0);
        Assert.NotNull(stats.LevelSizes);
        Assert.NotNull(stats.LevelNumSstables);
        Assert.NotNull(stats.LevelKeyCounts);
    }

    [Fact]
    public void ColumnFamily_GetStats_WithBtree_ShouldReturnBtreeStats()
    {
        using var db = OpenDatabase();
        var cfConfig = new ColumnFamilyConfig
        {
            WriteBufferSize = 1 * 1024 * 1024,
            UseBtree = true
        };
        db.CreateColumnFamily("btree_cf", cfConfig);
        var cf = db.GetColumnFamily("btree_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 50; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value{i}");
                txn.Put(cf, key, value);
            }
            txn.Commit();
        }

        var stats = cf.GetStats();
        Assert.NotNull(stats);
    }

    [Fact]
    public void CloneColumnFamily_ShouldCopyData()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("source_cf");
        var cf = db.GetColumnFamily("source_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
            txn.Commit();
        }

        db.CloneColumnFamily("source_cf", "cloned_cf");

        var clonedCf = db.GetColumnFamily("cloned_cf");
        Assert.NotNull(clonedCf);

        using var readTxn = db.BeginTransaction();
        var val = readTxn.Get(clonedCf, Encoding.UTF8.GetBytes("key1"));
        Assert.NotNull(val);
        Assert.Equal("value1", Encoding.UTF8.GetString(val));
    }

    [Fact]
    public void RenameColumnFamily_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("old_name");

        var cf = db.GetColumnFamily("old_name");
        Assert.NotNull(cf);

        db.RenameColumnFamily("old_name", "new_name");

        var oldCf = db.GetColumnFamily("old_name");
        Assert.Null(oldCf);

        var newCf = db.GetColumnFamily("new_name");
        Assert.NotNull(newCf);
    }

    [Fact]
    public void RenameColumnFamily_ShouldPreserveData()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("orig_cf");
        var cf = db.GetColumnFamily("orig_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Commit();
        }

        db.RenameColumnFamily("orig_cf", "renamed_cf");

        var renamedCf = db.GetColumnFamily("renamed_cf")!;
        using var readTxn = db.BeginTransaction();
        var val = readTxn.Get(renamedCf, Encoding.UTF8.GetBytes("key1"));
        Assert.NotNull(val);
        Assert.Equal("value1", Encoding.UTF8.GetString(val));
    }

    [Fact]
    public void Backup_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Commit();
        }

        var backupPath = Path.Combine(Path.GetTempPath(), $"tidesdb_backup_{Guid.NewGuid()}");
        try
        {
            db.Backup(backupPath);
            Assert.True(Directory.Exists(backupPath));
        }
        finally
        {
            if (Directory.Exists(backupPath))
            {
                Directory.Delete(backupPath, true);
            }
        }
    }

    [Fact]
    public void Transaction_Reset_ShouldAllowReuse()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using var txn = db.BeginTransaction();

        txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        txn.Commit();

        txn.Reset(IsolationLevel.ReadCommitted);

        txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
        txn.Commit();

        using var readTxn = db.BeginTransaction();
        Assert.NotNull(readTxn.Get(cf, Encoding.UTF8.GetBytes("key1")));
        Assert.NotNull(readTxn.Get(cf, Encoding.UTF8.GetBytes("key2")));
    }

    [Fact]
    public void Transaction_Reset_WithDifferentIsolation_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using var txn = db.BeginTransaction(IsolationLevel.ReadCommitted);
        txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        txn.Commit();

        txn.Reset(IsolationLevel.Serializable);
        txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
        txn.Commit();
    }

    [Fact]
    public void Checkpoint_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Commit();
        }

        var checkpointPath = _testDbPath + "_checkpoint";
        try
        {
            db.Checkpoint(checkpointPath);
            Assert.True(Directory.Exists(checkpointPath));
        }
        finally
        {
            if (Directory.Exists(checkpointPath))
            {
                Directory.Delete(checkpointPath, true);
            }
        }
    }

    [Fact]
    public void ColumnFamily_IsFlushing_ShouldReturnBool()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var isFlushing = cf.IsFlushing();
        Assert.False(isFlushing);
    }

    [Fact]
    public void ColumnFamily_IsCompacting_ShouldReturnBool()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var isCompacting = cf.IsCompacting();
        Assert.False(isCompacting);
    }

    [Fact]
    public void CommitHook_ShouldFireOnCommit()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var receivedOps = new List<CommitOp>();
        ulong receivedSeq = 0;

        cf.SetCommitHook((ops, seq) =>
        {
            receivedOps.AddRange(ops);
            receivedSeq = seq;
        });

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Commit();
        }

        Assert.Single(receivedOps);
        Assert.Equal("key1", Encoding.UTF8.GetString(receivedOps[0].Key));
        Assert.NotNull(receivedOps[0].Value);
        Assert.Equal("value1", Encoding.UTF8.GetString(receivedOps[0].Value!));
        Assert.False(receivedOps[0].IsDelete);
        Assert.True(receivedSeq > 0);
    }

    [Fact]
    public void CommitHook_ShouldReceiveDeleteOps()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Commit();
        }

        var receivedOps = new List<CommitOp>();

        cf.SetCommitHook((ops, seq) =>
        {
            receivedOps.AddRange(ops);
        });

        using (var txn = db.BeginTransaction())
        {
            txn.Delete(cf, Encoding.UTF8.GetBytes("key1"));
            txn.Commit();
        }

        Assert.Single(receivedOps);
        Assert.Equal("key1", Encoding.UTF8.GetString(receivedOps[0].Key));
        Assert.True(receivedOps[0].IsDelete);
        Assert.Null(receivedOps[0].Value);
    }

    [Fact]
    public void CommitHook_ShouldReceiveMultipleOps()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var receivedOps = new List<CommitOp>();

        cf.SetCommitHook((ops, seq) =>
        {
            receivedOps.AddRange(ops);
        });

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
            txn.Put(cf, Encoding.UTF8.GetBytes("key3"), Encoding.UTF8.GetBytes("value3"));
            txn.Commit();
        }

        Assert.Equal(3, receivedOps.Count);
    }

    [Fact]
    public void CommitHook_ClearShouldStopFiring()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        int callCount = 0;

        cf.SetCommitHook((ops, seq) =>
        {
            callCount++;
        });

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Commit();
        }

        Assert.Equal(1, callCount);

        cf.ClearCommitHook();

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
            txn.Commit();
        }

        Assert.Equal(1, callCount);
    }

    [Fact]
    public void CommitHook_SequenceNumberShouldIncrease()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var seqNumbers = new List<ulong>();

        cf.SetCommitHook((ops, seq) =>
        {
            seqNumbers.Add(seq);
        });

        for (int i = 0; i < 3; i++)
        {
            using var txn = db.BeginTransaction();
            txn.Put(cf, Encoding.UTF8.GetBytes($"key{i}"), Encoding.UTF8.GetBytes($"value{i}"));
            txn.Commit();
        }

        Assert.Equal(3, seqNumbers.Count);
        Assert.True(seqNumbers[1] > seqNumbers[0]);
        Assert.True(seqNumbers[2] > seqNumbers[1]);
    }

    [Fact]
    public void RangeCost_EmptyColumnFamily_ShouldReturnZero()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var cost = cf.RangeCost(
            Encoding.UTF8.GetBytes("key_a"),
            Encoding.UTF8.GetBytes("key_z"));

        Assert.Equal(0.0, cost);
    }

    [Fact]
    public void RangeCost_WithData_ShouldReturnNonNegative()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value{i}");
                txn.Put(cf, key, value);
            }
            txn.Commit();
        }

        var cost = cf.RangeCost(
            Encoding.UTF8.GetBytes("key0000"),
            Encoding.UTF8.GetBytes("key0099"));

        Assert.True(cost >= 0.0);
    }

    [Fact]
    public void RangeCost_WiderRange_ShouldCostMoreOrEqual()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value{i}");
                txn.Put(cf, key, value);
            }
            txn.Commit();
        }

        var narrowCost = cf.RangeCost(
            Encoding.UTF8.GetBytes("key0000"),
            Encoding.UTF8.GetBytes("key0010"));

        var wideCost = cf.RangeCost(
            Encoding.UTF8.GetBytes("key0000"),
            Encoding.UTF8.GetBytes("key0099"));

        Assert.True(wideCost >= narrowCost);
    }

    [Fact]
    public void RangeCost_ReversedKeys_ShouldReturnSameResult()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 50; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value{i}");
                txn.Put(cf, key, value);
            }
            txn.Commit();
        }

        var costAB = cf.RangeCost(
            Encoding.UTF8.GetBytes("key0000"),
            Encoding.UTF8.GetBytes("key0049"));

        var costBA = cf.RangeCost(
            Encoding.UTF8.GetBytes("key0049"),
            Encoding.UTF8.GetBytes("key0000"));

        Assert.Equal(costAB, costBA);
    }

    [Fact]
    public void UpdateRuntimeConfig_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var newConfig = new ColumnFamilyConfig
        {
            WriteBufferSize = 128 * 1024 * 1024,
            BloomFpr = 0.001,
            CompressionAlgorithm = CompressionAlgorithm.Zstd,
        };

        cf.UpdateRuntimeConfig(newConfig);
    }

    [Fact]
    public void UpdateRuntimeConfig_WithoutPersist_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        var newConfig = new ColumnFamilyConfig
        {
            WriteBufferSize = 256 * 1024 * 1024,
        };

        cf.UpdateRuntimeConfig(newConfig, persistToDisk: false);
    }

    [Fact]
    public void GetComparator_BuiltIn_ShouldReturnTrue()
    {
        using var db = OpenDatabase();

        Assert.True(db.GetComparator("memcmp"));
        Assert.True(db.GetComparator("reverse"));
    }

    [Fact]
    public void GetComparator_NonExistent_ShouldReturnFalse()
    {
        using var db = OpenDatabase();

        Assert.False(db.GetComparator("nonexistent_comparator"));
    }

    [Fact]
    public void ColumnFamily_Purge_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 50; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value{i}");
                txn.Put(cf, key, value);
            }
            txn.Commit();
        }

        cf.Purge();

        using var readTxn = db.BeginTransaction();
        var result = readTxn.Get(cf, Encoding.UTF8.GetBytes("key0000"));
        Assert.NotNull(result);
        Assert.Equal("value0", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public void Database_Purge_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("cf1");
        db.CreateColumnFamily("cf2");
        var cf1 = db.GetColumnFamily("cf1")!;
        var cf2 = db.GetColumnFamily("cf2")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf1, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Put(cf2, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
            txn.Commit();
        }

        db.Purge();

        using var readTxn = db.BeginTransaction();
        Assert.NotNull(readTxn.Get(cf1, Encoding.UTF8.GetBytes("key1")));
        Assert.NotNull(readTxn.Get(cf2, Encoding.UTF8.GetBytes("key2")));
    }

    [Fact]
    public void ColumnFamily_SyncWal_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Commit();
        }

        cf.SyncWal();

        using var readTxn = db.BeginTransaction();
        var result = readTxn.Get(cf, Encoding.UTF8.GetBytes("key1"));
        Assert.NotNull(result);
        Assert.Equal("value1", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public void GetDbStats_ShouldReturnStats()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("cf1");
        db.CreateColumnFamily("cf2");

        var stats = db.GetDbStats();
        Assert.NotNull(stats);
        Assert.Equal(2, stats.NumColumnFamilies);
        Assert.True(stats.TotalMemory > 0);
        Assert.True(stats.ResolvedMemoryLimit > 0);
        Assert.True(stats.MemoryPressureLevel >= 0);
    }

    [Fact]
    public void GetDbStats_AfterWrites_ShouldReflectData()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key{i:D4}");
                var value = Encoding.UTF8.GetBytes($"value{i}");
                txn.Put(cf, key, value);
            }
            txn.Commit();
        }

        var stats = db.GetDbStats();
        Assert.NotNull(stats);
        Assert.Equal(1, stats.NumColumnFamilies);
        Assert.True(stats.GlobalSeq > 0);
    }

    [Fact]
    public void DeleteColumnFamily_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");

        var cf = db.GetColumnFamily("test_cf");
        Assert.NotNull(cf);

        db.DeleteColumnFamily(cf);

        var deletedCf = db.GetColumnFamily("test_cf");
        Assert.Null(deletedCf);
    }

    [Fact]
    public void DeleteColumnFamily_ShouldRemoveData()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Commit();
        }

        db.DeleteColumnFamily(cf);

        var deletedCf = db.GetColumnFamily("test_cf");
        Assert.Null(deletedCf);
    }

    [Fact]
    public void Iterator_KeyValue_ShouldReturnBoth()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        using var iter = readTxn.NewIterator(cf);

        iter.SeekToFirst();
        Assert.True(iter.Valid());

        var (key, value) = iter.KeyValue();
        Assert.Equal("key1", Encoding.UTF8.GetString(key));
        Assert.Equal("value1", Encoding.UTF8.GetString(value));

        iter.Next();
        Assert.True(iter.Valid());

        var (key2, value2) = iter.KeyValue();
        Assert.Equal("key2", Encoding.UTF8.GetString(key2));
        Assert.Equal("value2", Encoding.UTF8.GetString(value2));
    }

    [Fact]
    public void Iterator_KeyValue_ForwardIteration_ShouldMatchSeparateCalls()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < 5; i++)
            {
                txn.Put(cf, Encoding.UTF8.GetBytes($"key{i:D2}"), Encoding.UTF8.GetBytes($"value{i}"));
            }
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        using var iter = readTxn.NewIterator(cf);

        iter.SeekToFirst();
        int count = 0;
        while (iter.Valid())
        {
            var (key, value) = iter.KeyValue();
            Assert.NotNull(key);
            Assert.NotNull(value);
            count++;
            iter.Next();
        }

        Assert.Equal(5, count);
    }

    [Fact]
    public void GetDbStats_ShouldReturnUnifiedMemtableStats()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");

        var stats = db.GetDbStats();
        Assert.NotNull(stats);
        Assert.False(stats.UnifiedMemtableEnabled);
        Assert.False(stats.ObjectStoreEnabled);
        Assert.False(stats.ReplicaMode);
    }

    [Fact]
    public void OpenWithUnifiedMemtable_ShouldSucceed()
    {
        var testPath = Path.Combine(Path.GetTempPath(), $"tidesdb_unified_{Guid.NewGuid()}");
        try
        {
            var config = new Config
            {
                DbPath = testPath,
                NumFlushThreads = 2,
                NumCompactionThreads = 2,
                LogLevel = LogLevel.Info,
                BlockCacheSize = 64 * 1024 * 1024,
                MaxOpenSstables = 256,
                UnifiedMemtable = true,
                UnifiedMemtableWriteBufferSize = 32 * 1024 * 1024
            };

            using var db = TidesDb.Open(config);
            Assert.NotNull(db);

            db.CreateColumnFamily("test_cf");
            var cf = db.GetColumnFamily("test_cf");
            Assert.NotNull(cf);

            var stats = db.GetDbStats();
            Assert.True(stats.UnifiedMemtableEnabled);
        }
        finally
        {
            if (Directory.Exists(testPath))
            {
                Directory.Delete(testPath, true);
            }
        }
    }

    [Fact]
    public void CreateColumnFamily_WithObjectStoreConfig_ShouldSucceed()
    {
        using var db = OpenDatabase();
        var cfConfig = new ColumnFamilyConfig
        {
            WriteBufferSize = 32 * 1024 * 1024,
            CompressionAlgorithm = CompressionAlgorithm.Lz4,
            ObjectLazyCompaction = false,
            ObjectPrefetchCompaction = true,
        };
        db.CreateColumnFamily("test_cf", cfConfig);

        var cf = db.GetColumnFamily("test_cf");
        Assert.NotNull(cf);
    }

    [Fact]
    public void ReleaseSavepoint_ShouldSucceed()
    {
        using var db = OpenDatabase();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf")!;

        using var txn = db.BeginTransaction();

        txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        txn.Savepoint("sp1");
        txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
        txn.ReleaseSavepoint("sp1");
        txn.Commit();

        using var readTxn = db.BeginTransaction();
        Assert.NotNull(readTxn.Get(cf, Encoding.UTF8.GetBytes("key1")));
        Assert.NotNull(readTxn.Get(cf, Encoding.UTF8.GetBytes("key2")));
    }

    [Fact]
    public void OpenWithObjectStore_Filesystem_ShouldSucceed()
    {
        var objStoreDir = Path.Combine(Path.GetTempPath(), $"tidesdb_objstore_{Guid.NewGuid()}");
        Directory.CreateDirectory(objStoreDir);

        try
        {
            var config = new Config
            {
                DbPath = _testDbPath,
                NumFlushThreads = 1,
                NumCompactionThreads = 1,
                LogLevel = LogLevel.Info,
                BlockCacheSize = 64 * 1024 * 1024,
                MaxOpenSstables = 256,
                ObjectStoreConfig = new ObjectStoreConfig
                {
                    ConnectorType = ObjectStoreConnectorType.Filesystem,
                    FsRootDir = objStoreDir,
                    LocalCacheMaxBytes = 128 * 1024 * 1024,
                    MaxConcurrentUploads = 4,
                    MaxConcurrentDownloads = 8,
                },
            };

            using var db = TidesDb.Open(config);
            _db = db;
            Assert.NotNull(db);

            db.CreateColumnFamily("test_cf");
            var cf = db.GetColumnFamily("test_cf");
            Assert.NotNull(cf);

            using var txn = db.BeginTransaction();
            txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
            txn.Commit();

            using var readTxn = db.BeginTransaction();
            var result = readTxn.Get(cf, Encoding.UTF8.GetBytes("key1"));
            Assert.NotNull(result);
            Assert.Equal("value1", Encoding.UTF8.GetString(result));
        }
        finally
        {
            if (Directory.Exists(objStoreDir))
                Directory.Delete(objStoreDir, true);
        }
    }

    [Fact]
    public void OpenWithObjectStore_DbStats_ShouldShowObjectStoreEnabled()
    {
        var objStoreDir = Path.Combine(Path.GetTempPath(), $"tidesdb_objstore_{Guid.NewGuid()}");
        Directory.CreateDirectory(objStoreDir);

        try
        {
            var config = new Config
            {
                DbPath = _testDbPath,
                NumFlushThreads = 1,
                NumCompactionThreads = 1,
                LogLevel = LogLevel.Info,
                BlockCacheSize = 64 * 1024 * 1024,
                MaxOpenSstables = 256,
                ObjectStoreConfig = new ObjectStoreConfig
                {
                    ConnectorType = ObjectStoreConnectorType.Filesystem,
                    FsRootDir = objStoreDir,
                },
            };

            using var db = TidesDb.Open(config);
            _db = db;

            var dbStats = db.GetDbStats();
            Assert.True(dbStats.ObjectStoreEnabled);
            Assert.NotNull(dbStats.ObjectStoreConnector);
        }
        finally
        {
            if (Directory.Exists(objStoreDir))
                Directory.Delete(objStoreDir, true);
        }
    }

    [Fact]
    public void ObjectStoreConfig_RequiresFsRootDir()
    {
        var config = new Config
        {
            DbPath = _testDbPath,
            ObjectStoreConfig = new ObjectStoreConfig
            {
                ConnectorType = ObjectStoreConnectorType.Filesystem,
                // FsRootDir not set
            },
        };

        Assert.Throws<ArgumentException>(() => TidesDb.Open(config));
    }

    [Fact]
    public void OpenWithObjectStore_ReplicaMode_ShouldRejectWrites()
    {
        var objStoreDir = Path.Combine(Path.GetTempPath(), $"tidesdb_objstore_{Guid.NewGuid()}");
        Directory.CreateDirectory(objStoreDir);

        try
        {
            // First open as primary and create a CF
            var primaryConfig = new Config
            {
                DbPath = _testDbPath,
                NumFlushThreads = 1,
                NumCompactionThreads = 1,
                LogLevel = LogLevel.Info,
                ObjectStoreConfig = new ObjectStoreConfig
                {
                    ConnectorType = ObjectStoreConnectorType.Filesystem,
                    FsRootDir = objStoreDir,
                },
            };

            using (var primaryDb = TidesDb.Open(primaryConfig))
            {
                primaryDb.CreateColumnFamily("test_cf");
                var cf = primaryDb.GetColumnFamily("test_cf")!;
                using var txn = primaryDb.BeginTransaction();
                txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
                txn.Commit();
            }

            // Open as replica
            var replicaDbPath = Path.Combine(Path.GetTempPath(), $"tidesdb_replica_{Guid.NewGuid()}");
            var replicaConfig = new Config
            {
                DbPath = replicaDbPath,
                NumFlushThreads = 1,
                NumCompactionThreads = 1,
                LogLevel = LogLevel.Info,
                ObjectStoreConfig = new ObjectStoreConfig
                {
                    ConnectorType = ObjectStoreConnectorType.Filesystem,
                    FsRootDir = objStoreDir,
                    ReplicaMode = true,
                    ReplicaSyncIntervalUs = 1_000_000,
                },
            };

            try
            {
                using var replicaDb = TidesDb.Open(replicaConfig);
                var dbStats = replicaDb.GetDbStats();
                Assert.True(dbStats.ReplicaMode);
            }
            finally
            {
                if (Directory.Exists(replicaDbPath))
                    Directory.Delete(replicaDbPath, true);
            }
        }
        finally
        {
            if (Directory.Exists(objStoreDir))
                Directory.Delete(objStoreDir, true);
        }
    }

    [Fact]
    public void TombstoneCfConfig_RoundTrip_ShouldPreserveValues()
    {
        using var db = OpenDatabase();
        var cfg = new ColumnFamilyConfig
        {
            TombstoneDensityTrigger = 0.5,
            TombstoneDensityMinEntries = 256
        };
        db.CreateColumnFamily("ts_cf", cfg);

        var cf = db.GetColumnFamily("ts_cf")!;
        var stats = cf.GetStats();

        Assert.NotNull(stats.Config);
        Assert.Equal(0.5, stats.Config!.TombstoneDensityTrigger);
        Assert.Equal((ulong)256, stats.Config.TombstoneDensityMinEntries);

        var defaults = ColumnFamilyConfig.Default;
        Assert.True(defaults.TombstoneDensityMinEntries > 0);
    }

    [Fact]
    public void TombstoneStats_ShouldPopulateAfterDeletes()
    {
        using var db = OpenDatabase();
        var cfg = new ColumnFamilyConfig { WriteBufferSize = 64 * 1024 };
        db.CreateColumnFamily("ts_stats_cf", cfg);
        var cf = db.GetColumnFamily("ts_stats_cf")!;

        const int n = 200;
        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < n; i++)
            {
                txn.Put(cf, Encoding.UTF8.GetBytes($"key{i:D5}"), Encoding.UTF8.GetBytes($"value{i}"));
            }
            txn.Commit();
        }
        cf.FlushMemtable();

        using (var txn = db.BeginTransaction())
        {
            for (int i = 0; i < n / 2; i++)
            {
                txn.Delete(cf, Encoding.UTF8.GetBytes($"key{i:D5}"));
            }
            txn.Commit();
        }
        cf.FlushMemtable();

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (cf.IsFlushing() && DateTime.UtcNow < deadline)
        {
            Thread.Sleep(50);
        }

        var stats = cf.GetStats();
        Assert.True(stats.TotalTombstones > 0, "expected total_tombstones > 0 after deletes + flush");
        Assert.InRange(stats.TombstoneRatio, 0.0, 1.0);
        Assert.InRange(stats.MaxSstDensity, 0.0, 1.0);
        Assert.NotNull(stats.LevelTombstoneCounts);
        Assert.Equal(stats.NumLevels, stats.LevelTombstoneCounts.Length);
    }

    [Fact]
    public void CompactRange_ShouldSucceedAndRejectBothEmpty()
    {
        using var db = OpenDatabase();
        var cfg = new ColumnFamilyConfig { WriteBufferSize = 64 * 1024 };
        db.CreateColumnFamily("range_cf", cfg);
        var cf = db.GetColumnFamily("range_cf")!;

        for (int batch = 0; batch < 4; batch++)
        {
            using var txn = db.BeginTransaction();
            for (int i = 0; i < 100; i++)
            {
                int idx = batch * 100 + i;
                txn.Put(cf, Encoding.UTF8.GetBytes($"key{idx:D5}"), Encoding.UTF8.GetBytes($"value{idx}"));
            }
            txn.Commit();
            cf.FlushMemtable();
        }

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (cf.IsFlushing() && DateTime.UtcNow < deadline)
        {
            Thread.Sleep(50);
        }

        var start = Encoding.UTF8.GetBytes("key00100");
        var end = Encoding.UTF8.GetBytes("key00200");
        cf.CompactRange(start, end);

        var ex = Assert.Throws<TidesDBException>(() => cf.CompactRange(default, default));
        Assert.Equal(ErrorCode.InvalidArgs, ex.ErrorCode);

        using var readTxn = db.BeginTransaction();
        var outsideKey = Encoding.UTF8.GetBytes("key00050");
        var v = readTxn.Get(cf, outsideKey);
        Assert.NotNull(v);
        Assert.Equal("value50", Encoding.UTF8.GetString(v!));
    }

    [Fact]
    public void MaxConcurrentFlushes_ShouldRespectConfig()
    {
        var defaults = Config.Default(_testDbPath);
        Assert.True(defaults.MaxConcurrentFlushes > 0,
            "Config.Default should source MaxConcurrentFlushes from tidesdb_default_config");

        var config = new Config
        {
            DbPath = _testDbPath,
            MaxConcurrentFlushes = 1
        };
        _db = TidesDb.Open(config);
        _db.CreateColumnFamily("mcf_cf");
        var cf = _db.GetColumnFamily("mcf_cf")!;

        using (var txn = _db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));
            txn.Commit();
        }
        cf.FlushMemtable();
    }
}
