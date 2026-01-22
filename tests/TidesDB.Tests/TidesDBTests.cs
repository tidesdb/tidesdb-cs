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

using System.Text;
using Xunit;

namespace TidesDB.Tests;

public class TidesDBTests : IDisposable
{
    private readonly string _testDbPath;
    private TidesDB? _db;

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

    private TidesDB OpenTestDb()
    {
        _db = TidesDB.Open(new Config
        {
            DbPath = _testDbPath,
            NumFlushThreads = 1,
            NumCompactionThreads = 1,
            LogLevel = LogLevel.Warn,
            BlockCacheSize = 16 * 1024 * 1024,
            MaxOpenSSTables = 64
        });
        return _db;
    }

    [Fact]
    public void OpenAndClose()
    {
        var db = OpenTestDb();
        Assert.NotNull(db);
        db.Close();
    }

    [Fact]
    public void CreateAndDropColumnFamily()
    {
        var db = OpenTestDb();
        
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");
        Assert.Equal("test_cf", cf.Name);

        var families = db.ListColumnFamilies();
        Assert.Contains("test_cf", families);

        db.DropColumnFamily("test_cf");
    }

    [Fact]
    public void CreateColumnFamilyWithConfig()
    {
        var db = OpenTestDb();
        
        db.CreateColumnFamily("custom_cf", new ColumnFamilyConfig
        {
            WriteBufferSize = 32 * 1024 * 1024,
            CompressionAlgorithm = CompressionAlgorithm.Lz4,
            EnableBloomFilter = true,
            BloomFpr = 0.01,
            SyncMode = SyncMode.Interval,
            SyncIntervalUs = 100000
        });

        var cf = db.GetColumnFamily("custom_cf");
        Assert.Equal("custom_cf", cf.Name);

        db.DropColumnFamily("custom_cf");
    }

    [Fact]
    public void PutAndGet()
    {
        var db = OpenTestDb();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");

        using var txn = db.BeginTransaction();
        txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        txn.Commit();

        using var readTxn = db.BeginTransaction();
        var value = readTxn.Get(cf, Encoding.UTF8.GetBytes("key1"));
        Assert.Equal("value1", Encoding.UTF8.GetString(value));

        db.DropColumnFamily("test_cf");
    }

    [Fact]
    public void PutAndDelete()
    {
        var db = OpenTestDb();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");

        using var txn = db.BeginTransaction();
        txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        txn.Commit();

        using var deleteTxn = db.BeginTransaction();
        deleteTxn.Delete(cf, Encoding.UTF8.GetBytes("key1"));
        deleteTxn.Commit();

        using var readTxn = db.BeginTransaction();
        Assert.Throws<TidesDBException>(() => readTxn.Get(cf, Encoding.UTF8.GetBytes("key1")));

        db.DropColumnFamily("test_cf");
    }

    [Fact]
    public void TransactionRollback()
    {
        var db = OpenTestDb();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");

        using var txn = db.BeginTransaction();
        txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        txn.Rollback();

        using var readTxn = db.BeginTransaction();
        Assert.Throws<TidesDBException>(() => readTxn.Get(cf, Encoding.UTF8.GetBytes("key1")));

        db.DropColumnFamily("test_cf");
    }

    [Fact]
    public void Savepoints()
    {
        var db = OpenTestDb();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");

        using var txn = db.BeginTransaction();
        txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        txn.Savepoint("sp1");
        txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"));
        txn.RollbackToSavepoint("sp1");
        txn.Commit();

        using var readTxn = db.BeginTransaction();
        var value1 = readTxn.Get(cf, Encoding.UTF8.GetBytes("key1"));
        Assert.Equal("value1", Encoding.UTF8.GetString(value1));
        Assert.Throws<TidesDBException>(() => readTxn.Get(cf, Encoding.UTF8.GetBytes("key2")));

        db.DropColumnFamily("test_cf");
    }

    [Fact]
    public void ForwardIteration()
    {
        var db = OpenTestDb();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("a"), Encoding.UTF8.GetBytes("1"));
            txn.Put(cf, Encoding.UTF8.GetBytes("b"), Encoding.UTF8.GetBytes("2"));
            txn.Put(cf, Encoding.UTF8.GetBytes("c"), Encoding.UTF8.GetBytes("3"));
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        using var iter = readTxn.NewIterator(cf);
        
        iter.SeekToFirst();
        var keys = new List<string>();
        while (iter.IsValid())
        {
            keys.Add(Encoding.UTF8.GetString(iter.Key()));
            iter.Next();
        }

        Assert.Equal(3, keys.Count);
        Assert.Equal("a", keys[0]);
        Assert.Equal("b", keys[1]);
        Assert.Equal("c", keys[2]);

        db.DropColumnFamily("test_cf");
    }

    [Fact]
    public void BackwardIteration()
    {
        var db = OpenTestDb();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("a"), Encoding.UTF8.GetBytes("1"));
            txn.Put(cf, Encoding.UTF8.GetBytes("b"), Encoding.UTF8.GetBytes("2"));
            txn.Put(cf, Encoding.UTF8.GetBytes("c"), Encoding.UTF8.GetBytes("3"));
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        using var iter = readTxn.NewIterator(cf);
        
        iter.SeekToLast();
        var keys = new List<string>();
        while (iter.IsValid())
        {
            keys.Add(Encoding.UTF8.GetString(iter.Key()));
            iter.Prev();
        }

        Assert.Equal(3, keys.Count);
        Assert.Equal("c", keys[0]);
        Assert.Equal("b", keys[1]);
        Assert.Equal("a", keys[2]);

        db.DropColumnFamily("test_cf");
    }

    [Fact]
    public void IteratorSeek()
    {
        var db = OpenTestDb();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");

        using (var txn = db.BeginTransaction())
        {
            txn.Put(cf, Encoding.UTF8.GetBytes("a"), Encoding.UTF8.GetBytes("1"));
            txn.Put(cf, Encoding.UTF8.GetBytes("c"), Encoding.UTF8.GetBytes("3"));
            txn.Put(cf, Encoding.UTF8.GetBytes("e"), Encoding.UTF8.GetBytes("5"));
            txn.Commit();
        }

        using var readTxn = db.BeginTransaction();
        using var iter = readTxn.NewIterator(cf);
        
        iter.Seek(Encoding.UTF8.GetBytes("b"));
        Assert.True(iter.IsValid());
        Assert.Equal("c", Encoding.UTF8.GetString(iter.Key()));

        db.DropColumnFamily("test_cf");
    }

    [Fact]
    public void GetCacheStats()
    {
        var db = OpenTestDb();
        var stats = db.GetCacheStats();
        Assert.True(stats.Enabled);
    }

    [Fact]
    public void GetColumnFamilyStats()
    {
        var db = OpenTestDb();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");

        var stats = cf.GetStats();
        Assert.True(stats.NumLevels >= 0);

        db.DropColumnFamily("test_cf");
    }

    [Fact]
    public void TransactionWithIsolationLevel()
    {
        var db = OpenTestDb();
        db.CreateColumnFamily("test_cf");
        var cf = db.GetColumnFamily("test_cf");

        using var txn = db.BeginTransactionWithIsolation(IsolationLevel.Serializable);
        txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"));
        txn.Commit();

        db.DropColumnFamily("test_cf");
    }
}
