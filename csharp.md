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
    LogToFile = false,
    LogTruncationAt = 0
};

using var db = TidesDb.Open(config);
Console.WriteLine("Database opened successfully");

// Database is automatically closed when disposed
```

### Creating and Dropping Column Families

Column families are isolated key-value stores with independent configuration.

```csharp
using TidesDB;

// Create with default configuration
db.CreateColumnFamily("my_cf");

// Create with custom configuration
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

// Drop a column family
db.DropColumnFamily("my_cf");
```

### CRUD Operations

All operations in TidesDB are performed through transactions for ACID guarantees.

#### Writing Data

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

// Put a key-value pair (TTL -1 means no expiration)
txn.Put(cf, Encoding.UTF8.GetBytes("key"), Encoding.UTF8.GetBytes("value"), -1);

txn.Commit();
```

#### Writing with TTL

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

// Set expiration time (Unix timestamp in seconds)
var ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 10; // Expire in 10 seconds

txn.Put(cf, Encoding.UTF8.GetBytes("temp_key"), Encoding.UTF8.GetBytes("temp_value"), ttl);

txn.Commit();
```

**TTL Examples**
```csharp
// No expiration
long ttl = -1;

// Expire in 5 minutes
long ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 5 * 60;

// Expire in 1 hour
long ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 60 * 60;

// Expire at specific time
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
    // Multiple operations in one transaction, across column families as well
    txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"), -1);
    txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"), -1);
    txn.Delete(cf, Encoding.UTF8.GetBytes("old_key"));

    // Commit atomically -- all or nothing
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
    
    // Stop when keys no longer match prefix
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

// Per-level statistics
for (int i = 0; i < stats.NumLevels; i++)
{
    Console.WriteLine($"Level {i + 1}: {stats.LevelNumSstables[i]} SSTables, {stats.LevelSizes[i]} bytes, {stats.LevelKeyCounts[i]} keys");
}

// B+tree stats (only populated if UseBtree=true)
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

### Compaction

#### Manual Compaction

```csharp
var cf = db.GetColumnFamily("my_cf")!;

// Manually trigger compaction (queues compaction from L1+)
cf.Compact();
```

#### Manual Memtable Flush

```csharp
var cf = db.GetColumnFamily("my_cf")!;

// Manually trigger memtable flush (queues memtable for sorted run to disk (L1))
cf.FlushMemtable();
```

### Sync Modes

Control the durability vs performance tradeoff.

```csharp
using TidesDB;

// SyncNone -- Fastest, least durable (OS handles flushing)
db.CreateColumnFamily("fast_cf", new ColumnFamilyConfig
{
    SyncMode = SyncMode.None,
});

// SyncInterval -- Balanced (periodic background syncing)
db.CreateColumnFamily("balanced_cf", new ColumnFamilyConfig
{
    SyncMode = SyncMode.Interval,
    SyncIntervalUs = 128000, // Sync every 128ms
});

// SyncFull -- Most durable (fsync on every write)
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

### B+tree Format (Optional)

Column families can optionally use a B+tree structure for the key log instead of the default block-based format. The B+tree format offers faster point lookups through O(log N) tree traversal.

```csharp
using TidesDB;

// Enable B+tree format for faster point lookups
db.CreateColumnFamily("btree_cf", new ColumnFamilyConfig
{
    UseBtree = true,
    CompressionAlgorithm = CompressionAlgorithm.Lz4,
    EnableBloomFilter = true,
});
```

**When to use B+tree format:**
- Read-heavy workloads with frequent point lookups
- Workloads where read latency is more important than write throughput
- Large SSTables where block scanning becomes expensive

**Tradeoffs:**
- Slightly higher write amplification during flush
- Larger metadata overhead per node
- Block-based format may be faster for sequential scans

**Important:** `UseBtree` cannot be changed after column family creation.

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
- `TDB_SUCCESS` (0) -- Operation successful
- `TDB_ERR_MEMORY` (-1) -- Memory allocation failed
- `TDB_ERR_INVALID_ARGS` (-2) -- Invalid arguments
- `TDB_ERR_NOT_FOUND` (-3) -- Key not found
- `TDB_ERR_IO` (-4) -- I/O error
- `TDB_ERR_CORRUPTION` (-5) -- Data corruption
- `TDB_ERR_EXISTS` (-6) -- Resource already exists
- `TDB_ERR_CONFLICT` (-7) -- Transaction conflict
- `TDB_ERR_TOO_LARGE` (-8) -- Key or value too large
- `TDB_ERR_MEMORY_LIMIT` (-9) -- Memory limit exceeded
- `TDB_ERR_INVALID_DB` (-10) -- Invalid database handle
- `TDB_ERR_UNKNOWN` (-11) -- Unknown error
- `TDB_ERR_LOCKED` (-12) -- Database is locked

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

    // Write data
    using (var txn = db.BeginTransaction())
    {
        txn.Put(cf, Encoding.UTF8.GetBytes("user:1"), Encoding.UTF8.GetBytes("Alice"), -1);
        txn.Put(cf, Encoding.UTF8.GetBytes("user:2"), Encoding.UTF8.GetBytes("Bob"), -1);

        // Temporary session with 30 second TTL
        var ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 30;
        txn.Put(cf, Encoding.UTF8.GetBytes("session:abc"), Encoding.UTF8.GetBytes("temp_data"), ttl);

        txn.Commit();
    }

    // Read data
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

    // Cleanup
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

// ... perform operations

txn.Commit();
```

**Available Isolation Levels**
- `IsolationLevel.ReadUncommitted` -- Sees all data including uncommitted changes
- `IsolationLevel.ReadCommitted` -- Sees only committed data (default)
- `IsolationLevel.RepeatableRead` -- Consistent snapshot, phantom reads possible
- `IsolationLevel.Snapshot` -- Write-write conflict detection
- `IsolationLevel.Serializable` -- Full read-write conflict detection (SSI)

## Savepoints

Savepoints allow partial rollback within a transaction:

```csharp
var cf = db.GetColumnFamily("my_cf")!;

using var txn = db.BeginTransaction();

txn.Put(cf, Encoding.UTF8.GetBytes("key1"), Encoding.UTF8.GetBytes("value1"), -1);

txn.Savepoint("sp1");
txn.Put(cf, Encoding.UTF8.GetBytes("key2"), Encoding.UTF8.GetBytes("value2"), -1);

// Rollback to savepoint -- key2 is discarded, key1 remains
txn.RollbackToSavepoint("sp1");

// Commit -- only key1 is written
txn.Commit();
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
// - TidesDb
// - ColumnFamily
// - Transaction
// - Iterator
// - TidesDBException

// Configuration classes
// - Config
// - ColumnFamilyConfig
// - Stats
// - CacheStats

// Enums
// - CompressionAlgorithm
// - SyncMode
// - LogLevel
// - IsolationLevel
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
| `UseBtree` | bool | Whether column family uses B+tree format |
| `BtreeTotalNodes` | ulong | Total B+tree nodes (only if UseBtree=true) |
| `BtreeMaxHeight` | uint | Maximum tree height (only if UseBtree=true) |
| `BtreeAvgHeight` | double | Average tree height (only if UseBtree=true) |