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

        var checkpointPath = Path.Combine(Path.GetTempPath(), $"tidesdb_checkpoint_{Guid.NewGuid()}");
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
}
