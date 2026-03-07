---
title: TidesDB C# API Reference
description: C# API reference for TidesDB
---

If you want to download the source of this document, you can find it [here](https://github.com/tidesdb/tidesdb.github.io/blob/master/src/content/docs/reference/csharp.md).

<hr/>

## Getting Started

### Prerequisites

You **must** have the TidesDB shared C library installed on your system. You can find the installation instructions [here](/reference/building/#_top).

### Installation

```bash
git clone https://github.com/tidesdb/tidesdb-cs.git
cd tidesdb-cs
dotnet build
```

### Custom Installation Paths

If you installed TidesDB to a non-standard location, you can specify custom paths using environment variables:

```bash
# Linux
export LD_LIBRARY_PATH="/custom/path/lib:$LD_LIBRARY_PATH"

# macOS
export DYLD_LIBRARY_PATH="/custom/path/lib:$DYLD_LIBRARY_PATH"

# Windows (add to PATH)
set PATH=C:\custom\path\bin;%PATH%
```

**Custom prefix installation**
```bash
# Install TidesDB to custom location
cd tidesdb
cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/opt/tidesdb
cmake --build build
sudo cmake --install build

# Configure environment to use custom location
export LD_LIBRARY_PATH="/opt/tidesdb/lib:$LD_LIBRARY_PATH"  # Linux
# or
export DYLD_LIBRARY_PATH="/opt/tidesdb/lib:$DYLD_LIBRARY_PATH"  # macOS
```

## Usage

### Opening and Closing a Database

```csharp
using TidesDB;

var config = new Config
{
    DbPath = "./mydb",
    NumFlushThreads = 2,
    NumCompactionThreads = 2,
    LogLevel = LogLevel.Info,
    BlockCacheSize = 64 * 1024 * 1024,
    MaxOpenSstables = 256,
    MaxMemoryUsage = 0, // 0 = auto (50% of system RAM)
    LogToFile = false,
    LogTruncationAt = 0
};

using var db = TidesDb.Open(config);
Console.WriteLine("Database opened successfully");

```

### Creating and Dropping Column Families

Column families are isolated key-value stores with independent configuration.

```csharp
using TidesDB;

db.CreateColumnFamily("my_cf");

db.CreateColumnFamily("my_cf", new ColumnFamilyConfig
{
    WriteBufferSize = 128 * 1024 * 1024,
    LevelSizeRatio = 10,
    MinLevels = 5,
    CompressionAlgorithm = CompressionAlgorithm.Lz4,
    EnableBloomFilter = true,
    BloomFpr = 0.01,
    EnableBlockIndexes = true,
    SyncMode = SyncMode.Interval,
    SyncIntervalUs = 128000,
    DefaultIsolationLevel = IsolationLevel.ReadCommitted,
    UseBtree = false, // Use B+tree format for klog (default: false = block-based)
});

db.DropColumnFamily("my_cf");
```

### Cloning a Column Family

Create a complete copy of an existing column family with a new name. The clone contains all the data from the source at the time of cloning.

```csharp
using TidesDB;

db.CreateColumnFamily("source_cf");

// Clone the column family
db.CloneColumnFamily("source_cf", "cloned_cf");

var original = db.GetColumnFamily("source_cf");
var clone = db.GetColumnFamily("cloned_cf");
```

**Use cases**
- Testing · Create a copy of production data for testing without affecting the original
- Branching · Create a snapshot of data before making experimental changes
- Migration · Clone data before schema or configuration changes

### Renaming a Column Family

Atomically rename a column family and its underlying directory.

```csharp
using TidesDB;

db.CreateColumnFamily("old_name");

db.RenameColumnFamily("old_name", "new_name");

var cf = db.GetColumnFamily("new_name");
```

### CRUD Operations

All operations in TidesDB are performed through transactions for ACID guarantees.

#### Writing Data

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

txn.Put(cf, Encoding.UTF8.GetBytes("key"), Encoding.UTF8.GetBytes("value"), -1);

txn.Commit();
```

#### Writing with TTL

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

var ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 10;

txn.Put(cf, Encoding.UTF8.GetBytes("temp_key"), Encoding.UTF8.GetBytes("temp_value"), ttl);

txn.Commit();
```

**TTL Examples**
```csharp
long ttl = -1;

long ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 5 * 60;

long ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 60 * 60;

long ttl = new DateTimeOffset(2026, 12, 31, 23, 59, 59, TimeSpan.Zero).ToUnixTimeSeconds();
```

#### Reading Data

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

var value = txn.Get(cf, Encoding.UTF8.GetBytes("key"));
if (value != null)
{
    Console.WriteLine($"Value: {Encoding.UTF8.GetString(value)}");
}
```

#### Deleting Data

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

txn.Delete(cf, Encoding.UTF8.GetBytes("key"));

txn.Commit();
```

#### Multi-Operation Transactions

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

try
{
    txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"), -1);
    txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"), -1);
    txn.Delete(cf, Encoding.UTF8.GetBytes("old_key"));

    txn.Commit();
}
catch
{
    txn.Rollback();
    throw;
}
```

### Iterating Over Data

Iterators provide efficient bidirectional traversal over key-value pairs.

#### Forward Iteration

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();
using var iter = txn.NewIterator(cf);

iter.SeekToFirst();

while (iter.Valid())
{
    var key = iter.Key();
    var value = iter.Value();
    
    Console.WriteLine($"Key: {Encoding.UTF8.GetString(key)}, Value: {Encoding.UTF8.GetString(value)}");
    
    iter.Next();
}
```

#### Backward Iteration

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();
using var iter = txn.NewIterator(cf);

iter.SeekToLast();

while (iter.Valid())
{
    var key = iter.Key();
    var value = iter.Value();
    
    Console.WriteLine($"Key: {Encoding.UTF8.GetString(key)}, Value: {Encoding.UTF8.GetString(value)}");
    
    iter.Prev();
}
```

#### Seek to Specific Key

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();
using var iter = txn.NewIterator(cf);

// Seek to first key >= target
iter.Seek(Encoding.UTF8.GetBytes("user:1000"));

if (iter.Valid())
{
    Console.WriteLine($"Found: {Encoding.UTF8.GetString(iter.Key())}");
}
```

#### Seek for Previous

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();
using var iter = txn.NewIterator(cf);

// Seek to last key <= target
iter.SeekForPrev(Encoding.UTF8.GetBytes("user:2000"));

while (iter.Valid())
{
    Console.WriteLine($"Key: {Encoding.UTF8.GetString(iter.Key())}");
    iter.Prev();
}
```

#### Prefix Scanning

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();
using var iter = txn.NewIterator(cf);

var prefix = "user:";
iter.Seek(Encoding.UTF8.GetBytes(prefix));

while (iter.Valid())
{
    var key = Encoding.UTF8.GetString(iter.Key());
    
    if (!key.StartsWith(prefix)) break;
    
    Console.WriteLine($"Found: {key}");
    iter.Next();
}
```

### Getting Column Family Statistics

Retrieve detailed statistics about a column family.

```csharp
var cf = db.GetColumnFamily("my_cf")!;

var stats = cf.GetStats();

Console.WriteLine($"Number of Levels: {stats.NumLevels}");
Console.WriteLine($"Memtable Size: {stats.MemtableSize} bytes");
Console.WriteLine($"Total Keys: {stats.TotalKeys}");
Console.WriteLine($"Total Data Size: {stats.TotalDataSize} bytes");
Console.WriteLine($"Avg Key Size: {stats.AvgKeySize:F1} bytes");
Console.WriteLine($"Avg Value Size: {stats.AvgValueSize:F1} bytes");
Console.WriteLine($"Read Amplification: {stats.ReadAmp:F2}");
Console.WriteLine($"Cache Hit Rate: {stats.HitRate * 100:F1}%");

for (int i = 0; i < stats.NumLevels; i++)
{
    Console.WriteLine($"Level {i + 1}: {stats.LevelNumSstables[i]} SSTables, {stats.LevelSizes[i]} bytes, {stats.LevelKeyCounts[i]} keys");
}

if (stats.UseBtree)
{
    Console.WriteLine($"B+tree Total Nodes: {stats.BtreeTotalNodes}");
    Console.WriteLine($"B+tree Max Height: {stats.BtreeMaxHeight}");
    Console.WriteLine($"B+tree Avg Height: {stats.BtreeAvgHeight:F2}");
}
```

### Listing Column Families

```csharp
var cfList = db.ListColumnFamilies();

Console.WriteLine("Available column families:");
foreach (var name in cfList)
{
    Console.WriteLine($"  - {name}");
}
```

### Updating Column Family Configuration

Update runtime-safe configuration settings for a column family. Changes apply to new operations only.

```csharp
var cf = db.GetColumnFamily("my_cf")!;

cf.UpdateRuntimeConfig(new ColumnFamilyConfig
{
    WriteBufferSize = 256 * 1024 * 1024,
    SkipListMaxLevel = 16,
    SkipListProbability = 0.25f,
    BloomFpr = 0.001,
    IndexSampleRatio = 8,
});
```

**Without persisting to disk**
```csharp
cf.UpdateRuntimeConfig(new ColumnFamilyConfig
{
    WriteBufferSize = 256 * 1024 * 1024,
}, persistToDisk: false);
```

**Updatable settings**
- `WriteBufferSize` · Memtable flush threshold
- `SkipListMaxLevel` · Skip list level for **new** memtables
- `SkipListProbability` · Skip list probability for **new** memtables
- `BloomFpr` · False positive rate for **new** SSTables
- `EnableBloomFilter` · Enable/disable bloom filters for **new** SSTables
- `EnableBlockIndexes` · Enable/disable block indexes for **new** SSTables
- `BlockIndexPrefixLen` · Block index prefix length for **new** SSTables
- `IndexSampleRatio` · Index sampling ratio for **new** SSTables
- `CompressionAlgorithm` · Compression for **new** SSTables (existing SSTables retain their original compression)
- `KlogValueThreshold` · Value log threshold for **new** writes
- `SyncMode` · Durability mode. Also updates the active WAL's sync mode immediately
- `SyncIntervalUs` · Sync interval in microseconds (only used when SyncMode is Interval)
- `LevelSizeRatio` · LSM level sizing
- `MinLevels` · Minimum LSM levels
- `DividingLevelOffset` · Compaction dividing level offset
- `L1FileCountTrigger` · L1 file count compaction trigger
- `L0QueueStallThreshold` · Backpressure stall threshold
- `DefaultIsolationLevel` · Default transaction isolation level
- `MinDiskSpace` · Minimum disk space required

**Non-updatable settings**
- `ComparatorName` · Cannot change sort order after creation
- `UseBtree` · Cannot change klog format after creation

:::tip[Important Notes]
Changes apply immediately to new operations. Existing SSTables and memtables retain their original settings. The update operation is thread-safe.
:::

### Compaction

#### Manual Compaction

```csharp
var cf = db.GetColumnFamily("my_cf")!;

cf.Compact();
```

#### Manual Memtable Flush

```csharp
var cf = db.GetColumnFamily("my_cf")!;

cf.FlushMemtable();
```

### Sync Modes

Control the durability vs performance tradeoff.

```csharp
using TidesDB;

db.CreateColumnFamily("fast_cf", new ColumnFamilyConfig
{
    SyncMode = SyncMode.None,
});

db.CreateColumnFamily("balanced_cf", new ColumnFamilyConfig
{
    SyncMode = SyncMode.Interval,
    SyncIntervalUs = 128000, // Sync every 128ms
});

db.CreateColumnFamily("durable_cf", new ColumnFamilyConfig
{
    SyncMode = SyncMode.Full,
});
```

### Compression Algorithms

TidesDB supports multiple compression algorithms:

```csharp
using TidesDB;

db.CreateColumnFamily("no_compress", new ColumnFamilyConfig
{
    CompressionAlgorithm = CompressionAlgorithm.None,
});

db.CreateColumnFamily("lz4_cf", new ColumnFamilyConfig
{
    CompressionAlgorithm = CompressionAlgorithm.Lz4,
});

db.CreateColumnFamily("lz4_fast_cf", new ColumnFamilyConfig
{
    CompressionAlgorithm = CompressionAlgorithm.Lz4Fast,
});

db.CreateColumnFamily("zstd_cf", new ColumnFamilyConfig
{
    CompressionAlgorithm = CompressionAlgorithm.Zstd,
});

db.CreateColumnFamily("snappy_cf", new ColumnFamilyConfig
{
    CompressionAlgorithm = CompressionAlgorithm.Snappy, // Not available on SunOS/Illumos/OmniOS
});
```

### B+tree KLog Format (Optional)

Column families can optionally use a B+tree structure for the key log instead of the default block-based format. The B+tree klog format offers faster point lookups through O(log N) tree traversal.

```csharp
using TidesDB;

db.CreateColumnFamily("btree_cf", new ColumnFamilyConfig
{
    UseBtree = true,
    CompressionAlgorithm = CompressionAlgorithm.Lz4,
    EnableBloomFilter = true,
});
```

**When to use B+tree klog format**
- Read-heavy workloads with frequent point lookups
- Workloads where read latency is more important than write throughput
- Large SSTables where block scanning becomes expensive

**Tradeoffs**
- Slightly higher write amplification during flush
- Larger metadata overhead per node
- Block-based format may be faster for sequential scans

:::caution[Important]
`UseBtree` cannot be changed after column family creation.
:::

## Error Handling

```csharp
using TidesDB;

var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

try
{
    txn.Put(cf, Encoding.UTF8.GetBytes("key"), Encoding.UTF8.GetBytes("value"), -1);
    txn.Commit();
}
catch (TidesDBException ex)
{
    Console.WriteLine($"Error code: {ex.ErrorCode}");
    Console.WriteLine($"Error message: {ex.Message}");
    
    txn.Rollback();
}
```

**Error Codes**
- `TDB_SUCCESS` (0) · Operation successful
- `TDB_ERR_MEMORY` (-1) · Memory allocation failed
- `TDB_ERR_INVALID_ARGS` (-2) · Invalid arguments
- `TDB_ERR_NOT_FOUND` (-3) · Key not found
- `TDB_ERR_IO` (-4) · I/O error
- `TDB_ERR_CORRUPTION` (-5) · Data corruption
- `TDB_ERR_EXISTS` (-6) · Resource already exists
- `TDB_ERR_CONFLICT` (-7) · Transaction conflict
- `TDB_ERR_TOO_LARGE` (-8) · Key or value too large
- `TDB_ERR_MEMORY_LIMIT` (-9) · Memory limit exceeded
- `TDB_ERR_INVALID_DB` (-10) · Invalid database handle
- `TDB_ERR_UNKNOWN` (-11) · Unknown error
- `TDB_ERR_LOCKED` (-12) · Database is locked

## Complete Example

```csharp
using System.Text;
using TidesDB;

var config = new Config
{
    DbPath = "./example_db",
    NumFlushThreads = 1,
    NumCompactionThreads = 1,
    LogLevel = LogLevel.Info,
    BlockCacheSize = 64 * 1024 * 1024,
    MaxOpenSstables = 256,
    MaxMemoryUsage = 0,
    LogToFile = false,
    LogTruncationAt = 0
};

using var db = TidesDb.Open(config);

try
{
    db.CreateColumnFamily("users", new ColumnFamilyConfig
    {
        WriteBufferSize = 64 * 1024 * 1024,
        CompressionAlgorithm = CompressionAlgorithm.Lz4,
        EnableBloomFilter = true,
        BloomFpr = 0.01,
        SyncMode = SyncMode.Interval,
        SyncIntervalUs = 128000,
    });

    var cf = db.GetColumnFamily("users")!;

    using (var txn = db.BeginTransaction())
    {
        txn.Put(cf, Encoding.UTF8.GetBytes("user:1"), Encoding.UTF8.GetBytes("Alice"), -1);
        txn.Put(cf, Encoding.UTF8.GetBytes("user:2"), Encoding.UTF8.GetBytes("Bob"), -1);

        // Temporary session with 30 second TTL
        var ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 30;
        txn.Put(cf, Encoding.UTF8.GetBytes("session:abc"), Encoding.UTF8.GetBytes("temp_data"), ttl);

        txn.Commit();
    }

    using (var txn = db.BeginTransaction())
    {
        var value = txn.Get(cf, Encoding.UTF8.GetBytes("user:1"));
        Console.WriteLine($"user:1 = {Encoding.UTF8.GetString(value!)}");

        // Iterate over all entries
        using var iter = txn.NewIterator(cf);

        Console.WriteLine("\nAll entries:");
        iter.SeekToFirst();
        while (iter.Valid())
        {
            var key = Encoding.UTF8.GetString(iter.Key());
            var val = Encoding.UTF8.GetString(iter.Value());
            Console.WriteLine($"  {key} = {val}");
            iter.Next();
        }
    }

    var stats = cf.GetStats();

    Console.WriteLine("\nColumn Family Statistics:");
    Console.WriteLine($"  Number of Levels: {stats.NumLevels}");
    Console.WriteLine($"  Memtable Size: {stats.MemtableSize} bytes");

    db.DropColumnFamily("users");
}
catch (TidesDBException ex)
{
    Console.WriteLine($"TidesDB error: {ex.Message} (code: {ex.ErrorCode})");
}
```

## Isolation Levels

TidesDB supports five MVCC isolation levels:

```csharp
using TidesDB;

using var txn = db.BeginTransaction(IsolationLevel.ReadCommitted);

txn.Commit();
```

**Available Isolation Levels**
- `IsolationLevel.ReadUncommitted` · Sees all data including uncommitted changes
- `IsolationLevel.ReadCommitted` · Sees only committed data (default)
- `IsolationLevel.RepeatableRead` · Consistent snapshot, phantom reads possible
- `IsolationLevel.Snapshot` · Write-write conflict detection
- `IsolationLevel.Serializable` · Full read-write conflict detection (SSI)

## Savepoints

Savepoints allow partial rollback within a transaction. You can create named savepoints, rollback to them, or release them without rolling back.

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"), -1);

txn.Savepoint("sp1");
txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"), -1);

txn.RollbackToSavepoint("sp1");

txn.Commit();
```

**Releasing a savepoint** (frees resources without rolling back)
```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"), -1);
txn.Savepoint("sp1");
txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"), -1);
txn.ReleaseSavepoint("sp1");
txn.Commit();
```

**Savepoint behavior**
- `Savepoint(name)` · Create a savepoint
- `RollbackToSavepoint(name)` · Rollback to savepoint, discarding all operations after it
- `ReleaseSavepoint(name)` · Release savepoint without rolling back
- Multiple savepoints can be created with different names
- Creating a savepoint with an existing name updates that savepoint
- Savepoints are automatically freed when the transaction commits or rolls back

## Transaction Reset

Reset a committed or aborted transaction for reuse with a new isolation level. This avoids the overhead of freeing and reallocating transaction resources in hot loops.

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"), -1);
txn.Commit();

txn.Reset(IsolationLevel.ReadCommitted);

txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"), -1);
txn.Commit();

```

**When to use**
- Batch processing · Reuse a single transaction across many commit cycles in a loop
- Connection pooling · Reset a transaction for a new request without reallocation
- High-throughput ingestion · Reduce allocation overhead in tight write loops

## Backup

Create an on-disk snapshot of an open database without blocking normal reads/writes.

```csharp
using TidesDB;

using var db = TidesDb.Open(config);

db.Backup("./mydb_backup");
```

**Behavior**
- Requires the directory to be non-existent or empty
- Does not copy the LOCK file, so the backup can be opened normally
- Database stays open and usable during backup

## Checkpoint

Create a lightweight, near-instant snapshot of an open database using hard links instead of copying SSTable data.

```csharp
using TidesDB;

using var db = TidesDb.Open(config);

db.Checkpoint("./mydb_checkpoint");
```

**Behavior**
- Requires the directory to be non-existent or empty
- Uses hard links for SSTable files (near-instant, O(1) per file)
- Falls back to file copy if hard linking fails (e.g., cross-filesystem)
- Flushes memtables and halts compactions to ensure a consistent snapshot
- Database stays open and usable during checkpoint

**Checkpoint vs Backup**

| | `Backup` | `Checkpoint` |
|--|---|---|
| Speed | Copies every SSTable byte-by-byte | Near-instant (hard links, O(1) per file) |
| Disk usage | Full independent copy | No extra disk until compaction removes old SSTables |
| Portability | Can be moved to another filesystem or machine | Same filesystem only (hard link requirement) |
| Use case | Archival, disaster recovery, remote shipping | Fast local snapshots, point-in-time reads, streaming backups |

**Notes**
- The checkpoint can be opened as a normal TidesDB database with `TidesDb.Open`
- Hard-linked files share storage with the live database; deleting the original does not affect the checkpoint

## Checking Flush/Compaction Status

Check if a column family currently has flush or compaction operations in progress.

```csharp
var cf = db.GetColumnFamily("my_cf")!;

if (cf.IsFlushing())
{
    Console.WriteLine("Flush in progress");
}

if (cf.IsCompacting())
{
    Console.WriteLine("Compaction in progress");
}
```

## Cache Statistics

```csharp
var cacheStats = db.GetCacheStats();

Console.WriteLine($"Cache enabled: {cacheStats.Enabled}");
Console.WriteLine($"Total entries: {cacheStats.TotalEntries}");
Console.WriteLine($"Total bytes: {cacheStats.TotalBytes / (1024.0 * 1024.0):F2} MB");
Console.WriteLine($"Hits: {cacheStats.Hits}");
Console.WriteLine($"Misses: {cacheStats.Misses}");
Console.WriteLine($"Hit rate: {cacheStats.HitRate * 100:F1}%");
Console.WriteLine($"Partitions: {cacheStats.NumPartitions}");
```

## Range Cost Estimation

`RangeCost` estimates the computational cost of iterating between two keys in a column family. The returned value is an opaque double — meaningful only for comparison with other values from the same method. It uses only in-memory metadata and performs no disk I/O.

```csharp
var cf = db.GetColumnFamily("my_cf")!;

var costA = cf.RangeCost(
    Encoding.UTF8.GetBytes("user:0000"),
    Encoding.UTF8.GetBytes("user:0999"));

var costB = cf.RangeCost(
    Encoding.UTF8.GetBytes("user:1000"),
    Encoding.UTF8.GetBytes("user:1099"));

if (costA < costB)
{
    Console.WriteLine("Range A is cheaper to iterate");
}
```

**How it works**

The function walks all SSTable levels and uses in-memory metadata to estimate how many blocks and entries fall within the given key range:

- With block indexes enabled · Uses O(log B) binary search per overlapping SSTable to find the block slots containing each key bound
- Without block indexes · Falls back to byte-level key interpolation against the SSTable's min/max key range
- B+tree SSTables (`UseBtree=true`) · Uses key interpolation against tree node counts, plus tree height as a seek cost
- Compression · Compressed SSTables receive a 1.5× weight multiplier to account for decompression overhead
- Merge overhead · Each overlapping SSTable adds a small fixed cost for merge-heap operations
- Memtable · The active memtable's entry count contributes a small in-memory cost

Key order does not matter — the method normalizes the range so `keyA > keyB` produces the same result as `keyB > keyA`.

**Use cases**
- Query planning · Compare candidate key ranges to find the cheapest one to scan
- Load balancing · Distribute range scan work across threads by estimating per-range cost
- Adaptive prefetching · Decide how aggressively to prefetch based on range size
- Monitoring · Track how data distribution changes across key ranges over time

:::note[Cost Values]
The returned cost is not an absolute measure (it does not represent milliseconds, bytes, or entry counts). It is a relative scalar — only meaningful when compared with other `RangeCost` results. A cost of 0.0 means no overlapping SSTables or memtable entries were found for the range.
:::

## Custom Comparators

TidesDB provides six built-in comparators that are automatically registered on database open:

- **`"memcmp"`** (default) · Binary byte-by-byte comparison
- **`"lexicographic"`** · Null-terminated string comparison
- **`"uint64"`** · Unsigned 64-bit integer comparison
- **`"int64"`** · Signed 64-bit integer comparison
- **`"reverse"`** · Reverse binary comparison
- **`"case_insensitive"`** · Case-insensitive ASCII comparison

### Registering a Comparator

```csharp
db.RegisterComparator("my_comparator");
```

### Getting a Registered Comparator

Check if a comparator is registered by name:

```csharp
if (db.GetComparator("memcmp"))
{
    Console.WriteLine("memcmp comparator is registered");
}

if (!db.GetComparator("nonexistent"))
{
    Console.WriteLine("comparator not registered");
}
```

**Use cases**
- Validation · Check if a comparator is registered before creating a column family
- Debugging · Verify comparator registration during development
- Dynamic configuration · Query available comparators at runtime

:::caution[Important]
Comparators must be registered before creating column families that use them. Once set, a comparator cannot be changed for a column family.
:::

## Commit Hook (Change Data Capture)

`SetCommitHook` registers a callback that fires synchronously after every transaction commit on a column family. The hook receives the full batch of committed operations atomically, enabling real-time change data capture without WAL parsing or external log consumers.

```csharp
var cf = db.GetColumnFamily("my_cf")!;

cf.SetCommitHook((ops, commitSeq) =>
{
    foreach (var op in ops)
    {
        if (op.IsDelete)
        {
            Console.WriteLine($"[{commitSeq}] DELETE {Encoding.UTF8.GetString(op.Key)}");
        }
        else
        {
            Console.WriteLine($"[{commitSeq}] PUT {Encoding.UTF8.GetString(op.Key)} = {Encoding.UTF8.GetString(op.Value!)}");
        }
    }
});

// Normal writes now trigger the hook automatically
using (var txn = db.BeginTransaction())
{
    txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"), -1);
    txn.Commit(); // hook fires here
}

// Detach hook
cf.ClearCommitHook();
```

**Operation fields**

| Property | Type | Description |
|----------|------|-------------|
| `Key` | byte[] | The key data |
| `Value` | byte[]? | The value data (null for deletes) |
| `Ttl` | long | Time-to-live (0 = no expiry) |
| `IsDelete` | bool | True if this is a delete operation |

**Behavior**
- The hook fires after WAL write, memtable apply, and commit status marking are complete — the data is fully durable before the callback runs
- Hook exceptions are caught internally and do not affect the commit result
- Each column family has its own independent hook; a multi-CF transaction fires the hook once per CF with only that CF's operations
- `commitSeq` is monotonically increasing across commits and can be used as a replication cursor
- Data in `CommitOp` is copied from native memory — safe to retain after the callback returns
- The hook executes synchronously on the committing thread; keep the callback fast to avoid stalling writers
- Calling `ClearCommitHook()` disables the hook immediately with no restart required

**Use cases**
- Replication · Ship committed batches to replicas in commit order
- Event streaming · Publish mutations to Kafka, NATS, or any message broker
- Secondary indexing · Maintain a reverse index or materialized view
- Audit logging · Record every mutation with key, value, TTL, and sequence number
- Debugging · Attach a temporary hook in production to inspect live writes

:::note[Runtime-Only]
Commit hooks are not persisted across database restarts. After reopening a database, hooks must be re-registered by the application. This is by design — delegates cannot be serialized.
:::

## Testing

```bash
cd tidesdb-cs
dotnet build
dotnet test
```

## C# Types

The package exports all necessary types for full C# support:

```csharp
using TidesDB;

// Main classes
// -- TidesDb
// -- ColumnFamily
// -- Transaction
// -- Iterator
// -- TidesDBException

// Configuration classes
// -- Config
// -- ColumnFamilyConfig
// -- Stats
// -- CacheStats

// Change data capture
// -- CommitOp
// -- CommitHookHandler

// Methods
// -- TidesDb.GetComparator(string name)
// -- ColumnFamily.UpdateRuntimeConfig(ColumnFamilyConfig config, bool persistToDisk)

// Enums
// -- CompressionAlgorithm
// -- SyncMode
// -- LogLevel
// -- IsolationLevel
```

## Configuration Reference

### Database Configuration (Config)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DbPath` | string | required | Path to the database directory |
| `NumFlushThreads` | int | 2 | Number of flush threads |
| `NumCompactionThreads` | int | 2 | Number of compaction threads |
| `LogLevel` | LogLevel | Info | Logging level |
| `BlockCacheSize` | ulong | 64MB | Block cache size in bytes |
| `MaxOpenSstables` | ulong | 256 | Maximum number of open SSTables |
| `MaxMemoryUsage` | ulong | 0 | Global memory limit in bytes (0 = auto, 50% of system RAM; minimum: 5% of system RAM) |
| `LogToFile` | bool | false | Write debug logging to a file |
| `LogTruncationAt` | ulong | 0 | Log file truncation threshold (0 = no truncation) |

### Column Family Configuration (ColumnFamilyConfig)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `WriteBufferSize` | ulong | 64MB | Memtable flush threshold |
| `LevelSizeRatio` | ulong | 10 | Level size multiplier |
| `MinLevels` | int | 5 | Minimum LSM levels |
| `DividingLevelOffset` | int | 2 | Compaction dividing level offset |
| `KlogValueThreshold` | ulong | 512 | Values larger than this go to vlog |
| `CompressionAlgorithm` | CompressionAlgorithm | Lz4 | Compression algorithm |
| `EnableBloomFilter` | bool | true | Enable bloom filters |
| `BloomFpr` | double | 0.01 | Bloom filter false positive rate |
| `EnableBlockIndexes` | bool | true | Enable block indexes |
| `IndexSampleRatio` | int | 1 | Index sample ratio |
| `BlockIndexPrefixLen` | int | 16 | Block index prefix length |
| `SyncMode` | SyncMode | Full | Sync mode for durability |
| `SyncIntervalUs` | ulong | 1000000 | Sync interval in microseconds |
| `ComparatorName` | string | "" | Comparator name (empty for default) |
| `SkipListMaxLevel` | int | 12 | Skip list max level |
| `SkipListProbability` | float | 0.25 | Skip list probability |
| `DefaultIsolationLevel` | IsolationLevel | ReadCommitted | Default transaction isolation |
| `MinDiskSpace` | ulong | 100MB | Minimum disk space required |
| `L1FileCountTrigger` | int | 4 | L1 file count trigger for compaction |
| `L0QueueStallThreshold` | int | 20 | L0 queue stall threshold |
| `UseBtree` | bool | false | Use B+tree format for klog |

### Column Family Statistics (Stats)

| Property | Type | Description |
|----------|------|-------------|
| `NumLevels` | int | Number of LSM levels |
| `MemtableSize` | ulong | Current memtable size in bytes |
| `LevelSizes` | ulong[] | Array of per-level total sizes |
| `LevelNumSstables` | int[] | Array of per-level SSTable counts |
| `LevelKeyCounts` | ulong[] | Array of per-level key counts |
| `Config` | ColumnFamilyConfig? | Full column family configuration |
| `TotalKeys` | ulong | Total keys across memtable and all SSTables |
| `TotalDataSize` | ulong | Total data size (klog + vlog) in bytes |
| `AvgKeySize` | double | Estimated average key size in bytes |
| `AvgValueSize` | double | Estimated average value size in bytes |
| `ReadAmp` | double | Read amplification factor (point lookup cost) |
| `HitRate` | double | Block cache hit rate (0.0 to 1.0) |
| `UseBtree` | bool | Whether column family uses B+tree klog format |
| `BtreeTotalNodes` | ulong | Total B+tree nodes (only if UseBtree=true) |
| `BtreeMaxHeight` | uint | Maximum tree height (only if UseBtree=true) |
| `BtreeAvgHeight` | double | Average tree height (only if UseBtree=true) |