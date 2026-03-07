---
title: TidesDB C API Reference
description: Complete C API reference for TidesDB
---

If you want to download the source of this document, you can find it [here](https://github.com/tidesdb/tidesdb.github.io/blob/master/src/content/docs/reference/c.md).

<hr/>


<details>
<summary>Want to watch a dedicated how-to video?</summary>

<iframe style="height: 420px;!important" width="720" height="420px" src="https://www.youtube.com/embed/uDIygMZTcLI?si=Axt_HSCFk8w8AKWx" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

</details>

## Overview

TidesDB is designed to provide a simple and intuitive C API for all your embedded storage needs.  This document is complete reference for the C API covering database operations, transactions, column families, iterators and more.

## Include

:::note
You can use other components of TidesDB such as skip list, bloom filter etc. under `tidesdb/` - this also prevents collisions.
:::

### Choosing Between `tidesdb.h` and `db.h`

TidesDB provides two header files with different purposes:

**tidesdb.h**

Full C implementation header. Mainly use this for native C/C++ applications.

```c
#include <tidesdb/tidesdb.h>
```

**db.h**

db.h is mainly an FFI/Language binding interface with minimal dependencies and simpler ABI.

```c
#include <tidesdb/db.h>
```

:::tip[When to Use Each]
- C/C++ applications ➞ Use `tidesdb.h` for full access to the API
- Language bindings (jextract, rust-bindgen, ctypes, cgo, etc.) ➞ Use `db.h` for a stable FFI interface
- Library developers ➞ Use `db.h` to avoid exposing internal implementation details
:::

## Error Codes

TidesDB provides detailed error codes for production use.

| Code | Value | Description |
|------|-------|-------------|
| `TDB_SUCCESS` | `0` | Operation completed successfully |
| `TDB_ERR_MEMORY` | `-1` | Memory allocation failed |
| `TDB_ERR_INVALID_ARGS` | `-2` | Invalid arguments passed to function (NULL pointers, invalid sizes, etc.) |
| `TDB_ERR_NOT_FOUND` | `-3` | Key not found in column family |
| `TDB_ERR_IO` | `-4` | I/O operation failed (file read/write error) |
| `TDB_ERR_CORRUPTION` | `-5` | Data corruption detected (checksum failure, invalid format version, truncated data) |
| `TDB_ERR_EXISTS` | `-6` | Resource already exists (e.g., column family name collision) |
| `TDB_ERR_CONFLICT` | `-7` | Transaction conflict detected (write-write or read-write conflict in SERIALIZABLE/SNAPSHOT isolation) |
| `TDB_ERR_TOO_LARGE` | `-8` | Key or value size exceeds maximum allowed size |
| `TDB_ERR_MEMORY_LIMIT` | `-9` | Operation would exceed memory limits (safety check to prevent OOM) |
| `TDB_ERR_INVALID_DB` | `-10` | Database handle is invalid (e.g., after close) |
| `TDB_ERR_UNKNOWN` | `-11` | Unknown or unspecified error |
| `TDB_ERR_LOCKED` | `-12` | Database is locked by another process |

**Error categories**
- `TDB_ERR_CORRUPTION` indicates data integrity issues requiring immediate attention
- `TDB_ERR_CONFLICT` indicates transaction conflicts (retry may succeed)
- `TDB_ERR_MEMORY`, `TDB_ERR_MEMORY_LIMIT`, `TDB_ERR_TOO_LARGE` indicate resource constraints
- `TDB_ERR_NOT_FOUND`, `TDB_ERR_EXISTS` are normal operational conditions, not failures

### Example Error Handling

```c
int result = tidesdb_txn_put(txn, cf, key, key_size, value, value_size, -1);
if (result != TDB_SUCCESS)
{
    switch (result)
    {
        case TDB_ERR_MEMORY:
            fprintf(stderr, "out of memory\n");
            break;
        case TDB_ERR_INVALID_ARGS:
            fprintf(stderr, "invalid arguments\n");
            break;
        case TDB_ERR_CONFLICT:
            fprintf(stderr, "transaction conflict detected\n");
            break;
        default:
            fprintf(stderr, "operation failed with error code: %d\n", result);
            break;
    }
    return -1;
}
```

## Initialization

TidesDB supports **optional** custom memory allocators for integration with custom memory managers (e.g., a Redis module allocator, jemalloc, etc).  You don't need to do this, this is completely optional.

### tidesdb_init

Initializes TidesDB with *optional* custom memory allocation functions. When in use, this must be called exactly once before any other TidesDB function. 

```c
int tidesdb_init(tidesdb_malloc_fn malloc_fn, tidesdb_calloc_fn calloc_fn,
                 tidesdb_realloc_fn realloc_fn, tidesdb_free_fn free_fn);
```

**Parameters**
| Name | Type | Description |
|------|------|-------------|
| `malloc_fn` | `tidesdb_malloc_fn` | Custom malloc function (or `NULL` for system malloc) |
| `calloc_fn` | `tidesdb_calloc_fn` | Custom calloc function (or `NULL` for system calloc) |
| `realloc_fn` | `tidesdb_realloc_fn` | Custom realloc function (or `NULL` for system realloc) |
| `free_fn` | `tidesdb_free_fn` | Custom free function (or `NULL` for system free) |

**Returns**
- `0` on success
- `-1` if already initialized

**Example (system allocator)**
```c
tidesdb_init(NULL, NULL, NULL, NULL);
```

**Example (custom allocator)**
```c
tidesdb_init(RedisModule_Alloc, RedisModule_Calloc,
             RedisModule_Realloc, RedisModule_Free);
```

:::note[Auto-initialization]
If `tidesdb_init()` is not called, TidesDB will auto-initialize with the system allocator on the first call to `tidesdb_open()`.
:::

### tidesdb_finalize

Finalizes TidesDB and resets the allocator. Should be called after all TidesDB operations are complete. After calling this, `tidesdb_init()` can be called again.

```c
void tidesdb_finalize(void);
```

**Example**
```c
tidesdb_init(NULL, NULL, NULL, NULL);

/* ... use TidesDB ... */

tidesdb_close(db);
tidesdb_finalize();
```

## Storage Engine Operations

### Opening TidesDB

```c
tidesdb_config_t config = {
    .db_path = "./mydb",
    .num_flush_threads = 2,                /* Flush thread pool size (default: 2) */
    .num_compaction_threads = 2,           /* Compaction thread pool size (default: 2) */
    .log_level = TDB_LOG_INFO,             /* Log level: TDB_LOG_DEBUG, TDB_LOG_INFO, TDB_LOG_WARN, TDB_LOG_ERROR, TDB_LOG_FATAL, TDB_LOG_NONE */
    .block_cache_size = 64 * 1024 * 1024,  /* 64MB global block cache (default: 64MB) */
    .max_open_sstables = 256,              /* Max cached SSTable structures (default: 256) */
    .max_memory_usage = 0,                 /* Global memory limit in bytes (default: 0 = auto, 50% of system RAM; minimum: 5% of system RAM) */
    .log_to_file = 0,                      /* Write logs to file instead of stderr (default: 0) */
    .log_truncation_at = 24 * (1024*1024), /* Log file truncation size (default: 24MB), 0 = no truncation */
};

tidesdb_t *db = NULL;
if (tidesdb_open(&config, &db) != 0)
{
    return -1;
}

if (tidesdb_close(db) != 0)
{
    return -1;
}
```

**Using default configuration**

Use `tidesdb_default_config()` to get a configuration with sensible defaults, then override specific fields as needed:

```c
tidesdb_config_t config = tidesdb_default_config();
config.db_path = "./mydb";
config.log_level = TDB_LOG_WARN;  /* Override: only warnings and errors */

tidesdb_t *db = NULL;
if (tidesdb_open(&config, &db) != 0)
{
    return -1;
}
```

:::note[Multiple DBs Allowed]
Multiple TidesDB instances can be opened in the same process, each with its own configuration and data directory.
:::

### Logging

TidesDB provides structured logging with multiple severity levels.

**Log Levels**
- `TDB_LOG_DEBUG` · Detailed diagnostic information
- `TDB_LOG_INFO` · General informational messages (default)
- `TDB_LOG_WARN` · Warning messages for potential issues
- `TDB_LOG_ERROR` · Error messages for failures
- `TDB_LOG_FATAL` · Critical errors that may cause shutdown
- `TDB_LOG_NONE` · Disable all logging

**Configure at startup**
```c
tidesdb_config_t config = {
    .db_path = "./mydb",
    .log_level = TDB_LOG_DEBUG  /* Enable debug logging */
};

tidesdb_t *db = NULL;
tidesdb_open(&config, &db);
```

**Production configuration**
```c
tidesdb_config_t config = {
    .db_path = "./mydb",
    .log_level = TDB_LOG_WARN  /* Only warnings and errors */
};
```

**Output format**
Logs are written to **stderr** by default with timestamps:
```
[HH:MM:SS.mmm] [LEVEL] filename:line: message
```

**Example output**
```
[22:58:00.454] [INFO] tidesdb.c:9322: Opening TidesDB with path=./mydb
[22:58:00.456] [INFO] tidesdb.c:9478: Block clock cache created with max_bytes=64.00 MB
```

**Log to file**

Enable `log_to_file` to write logs to a `LOG` file in the database directory instead of stderr:

```c
tidesdb_config_t config = {
    .db_path = "./mydb",
    .log_level = TDB_LOG_DEBUG,
    .log_to_file = 1  /* Write to ./mydb/LOG instead of stderr */
};

tidesdb_t *db = NULL;
tidesdb_open(&config, &db);
/* Logs are now written to ./mydb/LOG */
```

The log file is opened in append mode and uses line buffering for real-time logging. If the log file cannot be opened, logging falls back to default.

**Redirect stderr to file (alternative)**
```bash
./your_program 2> tidesdb.log  # Redirect std output to file
```

### Backup

`tidesdb_backup` creates an on-disk snapshot of an open database without blocking normal reads/writes.

```c
int tidesdb_backup(tidesdb_t *db, char *dir);
```

**Usage**
```c
tidesdb_t *db = NULL;
tidesdb_open(&config, &db);

if (tidesdb_backup(db, "./mydb_backup") != 0)
{
    fprintf(stderr, "Backup failed\n");
}
```

**Behavior**
- Requires `dir` to be a non-existent directory or an empty directory; returns `TDB_ERR_EXISTS` if not empty.
- Does not copy the `LOCK` file, so the backup can be opened normally.
- Two-phase copy approach:
  - Copies immutable files first (SSTables listed in the manifest plus metadata/config files) and skips WALs.
  - Forces memtable flushes, waits for flush/compaction queues to drain, then copies any remaining files
    (including WALs and updated manifests). Existing SSTables already copied are not recopied.
- Database stays open and usable during backup; no exclusive lock is taken on the source directory.

**Notes**
- The backup represents the database state after the final flush/compaction drain.
- If you need a quiesced backup window, you can pause writes at the application level before calling this API.

### Checkpoint

`tidesdb_checkpoint` creates a lightweight, near-instant snapshot of an open database using hard links instead of copying SSTable data.

```c
int tidesdb_checkpoint(tidesdb_t *db, const char *checkpoint_dir);
```

**Usage**
```c
tidesdb_t *db = NULL;
tidesdb_open(&config, &db);

if (tidesdb_checkpoint(db, "./mydb_checkpoint") != 0)
{
    fprintf(stderr, "Checkpoint failed\n");
}
```

**Behavior**
- Requires `checkpoint_dir` to be a non-existent directory or an empty directory; returns `TDB_ERR_EXISTS` if not empty.
- For each column family:
  - Flushes the active memtable so all data is in SSTables.
  - Halts compactions to ensure a consistent view of live SSTable files.
  - Hard links all SSTable files (`.klog` and `.vlog`) into the checkpoint directory, preserving the level subdirectory structure.
  - Copies small metadata files (manifest, config) into the checkpoint directory.
  - Resumes compactions.
- Falls back to file copy if hard linking fails (e.g., cross-filesystem).
- Database stays open and usable during checkpoint; no exclusive lock is taken on the source directory.

**Checkpoint vs Backup**

| | `tidesdb_backup` | `tidesdb_checkpoint` |
|--|---|---|
| Speed | Copies every SSTable byte-by-byte | Near-instant (hard links, O(1) per file) |
| Disk usage | Full independent copy | No extra disk until compaction removes old SSTables |
| Portability | Can be moved to another filesystem or machine | Same filesystem only (hard link requirement) |
| Use case | Archival, disaster recovery, remote shipping | Fast local snapshots, point-in-time reads, streaming backups |

**Notes**
- The checkpoint represents the database state at the point all memtables are flushed and compactions are halted.
- Hard-linked files share storage with the live database. Deleting the original database does not affect the checkpoint (hard link semantics).
- The checkpoint can be opened as a normal TidesDB database with `tidesdb_open`.

## Column Family Operations

### Creating a Column Family

Column families are isolated key-value stores. Use the config struct for customization or use defaults.

```c
/* Create with default configuration */
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();

if (tidesdb_create_column_family(db, "my_cf", &cf_config) != 0)
{
    return -1;
}
```

**Custom configuration example**
```c
tidesdb_column_family_config_t cf_config = {
    .write_buffer_size = 128 * 1024 * 1024,     /* 128MB memtable flush threshold */
    .level_size_ratio = 10,                     /* Level size multiplier (default: 10) */
    .min_levels = 5,                            /* Minimum LSM levels (default: 5) */
    .dividing_level_offset = 2,                 /* Compaction dividing level offset (default: 2) */
    .skip_list_max_level = 12,                  /* Skip list max level */
    .skip_list_probability = 0.25f,             /* Skip list probability */
    .compression_algorithm = TDB_COMPRESS_LZ4,  /* TDB_COMPRESS_LZ4, TDB_COMPRESS_LZ4_FAST, TDB_COMPRESS_ZSTD, TDB_COMPRESS_SNAPPY, or TDB_COMPRESS_NONE */
    .enable_bloom_filter = 1,                   /* Enable bloom filters */
    .bloom_fpr = 0.01,                          /* 1% false positive rate */
    .enable_block_indexes = 1,                  /* Enable compact block indexes */
    .index_sample_ratio = 1,                    /* Sample every block for index (default: 1) */
    .block_index_prefix_len = 16,               /* Block index prefix length (default: 16) */
    .sync_mode = TDB_SYNC_FULL,                 /* TDB_SYNC_NONE, TDB_SYNC_INTERVAL, or TDB_SYNC_FULL */
    .sync_interval_us = 1000000,                /* Sync interval in microseconds (1 second, only for TDB_SYNC_INTERVAL) */
    .comparator_name = {0},                     /* Empty = use default "memcmp" */
    .klog_value_threshold = 512,                /* Values >= 512 bytes go to vlog (default: 512) */
    .min_disk_space = 100 * 1024 * 1024,        /* Minimum disk space required (default: 100MB) */
    .default_isolation_level = TDB_ISOLATION_READ_COMMITTED,  /* Default transaction isolation */
    .l1_file_count_trigger = 4,                 /* L1 file count trigger for compaction (default: 4) */
    .l0_queue_stall_threshold = 20,             /* L0 queue stall threshold (default: 20) */
    .use_btree = 0                              /* Use B+tree format for klog (default: 0 = block-based) */
};

if (tidesdb_create_column_family(db, "my_cf", &cf_config) != 0)
{
    return -1;
}
```

**Using custom comparator**
```c
/* Register comparator after opening database but before creating CF */
tidesdb_register_comparator(db, "reverse", my_reverse_compare, NULL, NULL);

tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
strncpy(cf_config.comparator_name, "reverse", TDB_MAX_COMPARATOR_NAME - 1);  
cf_config.comparator_name[TDB_MAX_COMPARATOR_NAME - 1] = '\0';

if (tidesdb_create_column_family(db, "sorted_cf", &cf_config) != 0)
{
    return -1;
}
```

### Dropping a Column Family

By name (looks up column family internally):

```c
if (tidesdb_drop_column_family(db, "my_cf") != 0)
{
    return -1;
}
```

By pointer (skips name lookup when you already have the pointer):

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

if (tidesdb_delete_column_family(db, cf) != 0)
{
    return -1;
}
```

:::tip[Which to use]
- `tidesdb_drop_column_family(db, name)` · convenient when you only have the name
- `tidesdb_delete_column_family(db, cf)` · faster when you already hold a `tidesdb_column_family_t*`, avoids a redundant linear scan
:::

### Renaming a Column Family

Atomically rename a column family and its underlying directory. The operation waits for any in-progress flush or compaction to complete before renaming.

```c
if (tidesdb_rename_column_family(db, "old_name", "new_name") != 0)
{
    return -1;
}
```

**Behavior**
- Waits for any in-progress flush or compaction to complete
- Atomically renames the column family directory on disk
- Updates all internal paths (SSTables, manifest, config)
- Thread-safe with proper locking

**Return values**
- `TDB_SUCCESS` · Rename completed successfully
- `TDB_ERR_NOT_FOUND` · Column family with `old_name` doesn't exist
- `TDB_ERR_EXISTS` · Column family with `new_name` already exists
- `TDB_ERR_IO` · Failed to rename directory on disk

### Cloning a Column Family

Create a complete copy of an existing column family with a new name. The clone contains all the data from the source at the time of cloning.

```c
if (tidesdb_clone_column_family(db, "source_cf", "cloned_cf") != 0)
{
    return -1;
}

/* Both column families now exist independently */
tidesdb_column_family_t *original = tidesdb_get_column_family(db, "source_cf");
tidesdb_column_family_t *clone = tidesdb_get_column_family(db, "cloned_cf");
```

**Behavior**
- Flushes the source column family's memtable to ensure all data is on disk
- Waits for any in-progress flush or compaction to complete
- Copies all SSTable files (`.klog` and `.vlog`) to the new directory
- Copies manifest and configuration files
- Creates a new column family structure and loads the copied SSTables
- The clone is completely independent - modifications to one do not affect the other

**Use cases**
- Testing · Create a copy of production data for testing without affecting the original
- Branching · Create a snapshot of data before making experimental changes
- Migration · Clone data before schema or configuration changes
- Backup verification · Clone and verify data integrity without modifying the source

**Return values**
- `TDB_SUCCESS` · Clone completed successfully
- `TDB_ERR_NOT_FOUND` · Source column family doesn't exist
- `TDB_ERR_EXISTS` · Destination column family already exists
- `TDB_ERR_INVALID_ARGS` · Invalid arguments (NULL pointers or same source/destination name)
- `TDB_ERR_IO` · Failed to copy files or create directory

:::note[Clone vs Backup]
`tidesdb_clone_column_family` creates a new column family within the same database instance. For creating an external backup of the entire database, use `tidesdb_backup` instead.
:::

### Getting a Column Family

Retrieve a column family pointer to use in operations.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (cf == NULL)
{
    /* Column family not found */
    return -1;
}
```

### Listing Column Families

Get all column family names on the TidesDB instance.

```c
char **names = NULL;
int count = 0;

if (tidesdb_list_column_families(db, &names, &count) == 0)
{
    printf("Found %d column families:\n", count);
    for (int i = 0; i < count; i++)
    {
        printf("  - %s\n", names[i]);
        free(names[i]);
    }
    free(names);
}
```

### Column Family Statistics

Get detailed statistics about a column family.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_stats_t *stats = NULL;
if (tidesdb_get_stats(cf, &stats) == 0)
{
    printf("Memtable Size: %zu bytes\n", stats->memtable_size);
    printf("Number of Levels: %d\n", stats->num_levels);
    printf("Total Keys: %" PRIu64 "\n", stats->total_keys);
    printf("Total Data Size: %" PRIu64 " bytes\n", stats->total_data_size);
    printf("Avg Key Size: %.1f bytes\n", stats->avg_key_size);
    printf("Avg Value Size: %.1f bytes\n", stats->avg_value_size);
    printf("Read Amplification: %.2f\n", stats->read_amp);
    printf("Cache Hit Rate: %.1f%%\n", stats->hit_rate * 100.0);
    
    /* Column family name is available via config */
    printf("Column Family: %s\n", stats->config->name);
    
    for (int i = 0; i < stats->num_levels; i++)
    {
        printf("Level %d: %d SSTables, %zu bytes, %" PRIu64 " keys\n", 
               i + 1, stats->level_num_sstables[i], stats->level_sizes[i],
               stats->level_key_counts[i]);
    }
    
    /* B+tree stats (only populated if use_btree=1) */
    if (stats->use_btree)
    {
        printf("B+tree Total Nodes: %" PRIu64 "\n", stats->btree_total_nodes);
        printf("B+tree Max Height: %u\n", stats->btree_max_height);
        printf("B+tree Avg Height: %.2f\n", stats->btree_avg_height);
    }
    
    /* Access configuration */
    printf("Write Buffer Size: %zu\n", stats->config->write_buffer_size);
    printf("Compression: %d\n", stats->config->compression_algorithm);
    printf("Bloom Filter: %s\n", stats->config->enable_bloom_filter ? "enabled" : "disabled");
    
    tidesdb_free_stats(stats);
}
```

**Statistics include**

| Field | Type | Description |
|-------|------|-------------|
| `num_levels` | `int` | Number of LSM levels |
| `memtable_size` | `size_t` | Current memtable size in bytes |
| `level_sizes` | `size_t*` | Array of per-level total sizes |
| `level_num_sstables` | `int*` | Array of per-level SSTable counts |
| `level_key_counts` | `uint64_t*` | Array of per-level key counts |
| `config` | `tidesdb_column_family_config_t*` | Full column family configuration. This includes column family name if you need it! |
| `total_keys` | `uint64_t` | Total keys across memtable and all SSTables |
| `total_data_size` | `uint64_t` | Total data size (klog + vlog) in bytes |
| `avg_key_size` | `double` | Estimated average key size in bytes |
| `avg_value_size` | `double` | Estimated average value size in bytes |
| `read_amp` | `double` | Read amplification factor (point lookup cost) |
| `hit_rate` | `double` | Block cache hit rate (0.0 to 1.0) |
| `use_btree` | `int` | Whether column family uses B+tree format |
| `btree_total_nodes` | `uint64_t` | Total B+tree nodes across all SSTables |
| `btree_max_height` | `uint32_t` | Maximum tree height across all SSTables |
| `btree_avg_height` | `double` | Average tree height across all SSTables |

:::tip[B+tree Statistics]
The B+tree stats (`btree_total_nodes`, `btree_max_height`, `btree_avg_height`) are only populated when `use_btree=1` in the column family configuration. These provide insight into the index structure overhead and lookup depth.
:::

### Block Cache Statistics

Get statistics for the global block cache (shared across all column families).

```c
tidesdb_cache_stats_t cache_stats;
if (tidesdb_get_cache_stats(db, &cache_stats) == 0)
{
    if (cache_stats.enabled)
    {
        printf("Cache enabled: yes\n");
        printf("Total entries: %zu\n", cache_stats.total_entries);
        printf("Total bytes: %.2f MB\n", cache_stats.total_bytes / (1024.0 * 1024.0));
        printf("Hits: %lu\n", cache_stats.hits);
        printf("Misses: %lu\n", cache_stats.misses);
        printf("Hit rate: %.1f%%\n", cache_stats.hit_rate * 100.0);
        printf("Partitions: %zu\n", cache_stats.num_partitions);
    }
    else
    {
        printf("Cache enabled: no (block_cache_size = 0)\n");
    }
}
```

**Cache statistics include**
- `enabled` · Whether block cache is active (0 if `block_cache_size` was set to 0)
- `total_entries` · Number of cached blocks
- `total_bytes` · Total memory used by cached blocks
- `hits` · Number of cache hits (blocks served from memory)
- `misses` · Number of cache misses (blocks read from disk)
- `hit_rate` · Hit rate as a decimal (0.0 to 1.0)
- `num_partitions` · Number of cache partitions (scales with CPU cores)

:::note[Block Cache]
The block cache is a database-level resource shared across all column families. It caches deserialized klog blocks to avoid repeated disk I/O and deserialization. Configure cache size via `config.block_cache_size` when opening the database. Set to 0 to disable caching.
:::

### Range Cost Estimation

`tidesdb_range_cost` estimates the computational cost of iterating between two keys in a column family. The returned value is an opaque double — meaningful only for comparison with other values from the same function. It uses only in-memory metadata and performs no disk I/O.

```c
int tidesdb_range_cost(tidesdb_column_family_t *cf,
                       const uint8_t *key_a, size_t key_a_size,
                       const uint8_t *key_b, size_t key_b_size,
                       double *cost);
```

**Parameters**
| Name | Type | Description |
|------|------|-------------|
| `cf` | `tidesdb_column_family_t*` | Column family to estimate cost for |
| `key_a` | `const uint8_t*` | First key (bound of range) |
| `key_a_size` | `size_t` | Size of first key |
| `key_b` | `const uint8_t*` | Second key (bound of range) |
| `key_b_size` | `size_t` | Size of second key |
| `cost` | `double*` | Output: estimated traversal cost (higher = more expensive) |

**Returns**
- `TDB_SUCCESS` on success
- `TDB_ERR_INVALID_ARGS` on bad input (NULL pointers, zero-length keys)

**Example**
```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

double cost_a = 0.0, cost_b = 0.0;

tidesdb_range_cost(cf, (uint8_t *)"user:0000", 9,
                       (uint8_t *)"user:0999", 9, &cost_a);

tidesdb_range_cost(cf, (uint8_t *)"user:1000", 9,
                       (uint8_t *)"user:1099", 9, &cost_b);

if (cost_a < cost_b)
{
    printf("Range A is cheaper to iterate\n");
}
```

**How it works**

The function walks all SSTable levels and uses in-memory metadata to estimate how many blocks and entries fall within the given key range:

- With block indexes enabled · Uses O(log B) binary search per overlapping SSTable to find the block slots containing each key bound. The block span between slots, scaled by `index_sample_ratio`, gives the estimated block count.
- Without block indexes · Falls back to byte-level key interpolation. The leading 8 bytes of each key are converted to a numeric position within the SSTable's min/max key range to estimate the fraction of blocks covered.
- B+tree SSTables (`use_btree=1`) · Uses the same key interpolation against tree node counts, plus tree height as a seek cost. Only applies to column families configured with B+tree klog format.
- Compression · Compressed SSTables receive a 1.5× weight multiplier to account for decompression overhead.
- Merge overhead · Each overlapping SSTable adds a small fixed cost for merge-heap operations.
- Memtable · The active memtable's entry count contributes a small in-memory cost.

Key order does not matter — the function normalizes the range so `key_a > key_b` produces the same result as `key_b > key_a`.

**Use cases**
- Query planning · Compare candidate key ranges to find the cheapest one to scan
- Load balancing · Distribute range scan work across threads by estimating per-range cost
- Adaptive prefetching · Decide how aggressively to prefetch based on range size
- Monitoring · Track how data distribution changes across key ranges over time

:::note[Cost Values]
The returned cost is not an absolute measure (it does not represent milliseconds, bytes, or entry counts). It is a relative scalar — only meaningful when compared with other `tidesdb_range_cost` results. A cost of 0.0 means no overlapping SSTables or memtable entries were found for the range.
:::

### Compression Algorithms

TidesDB supports multiple compression algorithms to reduce storage footprint and I/O bandwidth. Compression is applied to both klog (key-log) and vlog (value-log) blocks before writing to disk.

**Available Algorithms**

- **`TDB_COMPRESS_NONE`** · No compression (value: 0)
  - Raw data written directly to disk
  - **Use case** · Pre-compressed data, maximum write throughput, CPU-constrained environments

- **`TDB_COMPRESS_LZ4`** · LZ4 standard compression (value: 2, **default**)
  - Fast compression and decompression with good compression ratios
  - **Use case** · General purpose, balanced performance and compression
  - **Performance** · ~500 MB/s compression, ~2000 MB/s decompression (typical)

- **`TDB_COMPRESS_LZ4_FAST`** · LZ4 fast mode (value: 4)
  - Faster compression than standard LZ4 with slightly lower compression ratio
  - Uses acceleration factor of 2
  - **Use case** · Write-heavy workloads prioritizing speed over compression ratio
  - **Performance** · Higher compression throughput than standard LZ4

- **`TDB_COMPRESS_ZSTD`** · Zstandard compression (value: 3)
  - Best compression ratio with moderate speed (compression level 1)
  - **Use case** · Storage-constrained environments, archival data, read-heavy workloads
  - **Performance** · ~400 MB/s compression, ~1000 MB/s decompression (typical)

- **`TDB_COMPRESS_SNAPPY`** · Snappy compression (value: 1)
  - Fast compression with moderate compression ratios
  - **Availability** · Not available on SunOS/Illumos/OmniOS platforms
  - **Use case** · Legacy compatibility, platforms where Snappy is preferred

**Configuration Example**

```c
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();

/* Use LZ4 compression (default) */
cf_config.compression_algorithm = TDB_COMPRESS_LZ4;

/* Use Zstandard for better compression ratio */
cf_config.compression_algorithm = TDB_COMPRESS_ZSTD;

/* Use LZ4 fast mode for maximum write throughput */
cf_config.compression_algorithm = TDB_COMPRESS_LZ4_FAST;

/* Disable compression */
cf_config.compression_algorithm = TDB_COMPRESS_NONE;

tidesdb_create_column_family(db, "my_cf", &cf_config);
```

:::caution[Important]
Compression algorithm can be changed at runtime via `tidesdb_cf_update_runtime_config`, but the change only affects **new** SSTables. Existing SSTables retain their original compression and are decompressed correctly during reads. Compression is applied at the block level (both klog and vlog blocks).
:::
- Decompression happens automatically during reads
- Block cache stores **decompressed** blocks to avoid repeated decompression overhead
- Different column families can use different compression algorithms

### B+tree KLog Format (Optional)

Column families can optionally use a B+tree structure for the key log instead of the default block-based format. The B+tree format offers faster point lookups through O(log N) tree traversal rather than linear block scanning.

**Enabling B+tree format**

```c
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
cf_config.use_btree = 1;  /* Enable B+tree format */

tidesdb_create_column_family(db, "btree_cf", &cf_config);
```

**Characteristics**
- Point lookups · O(log N) tree traversal with binary search at each node, compared to potentially scanning multiple 64KB blocks in block-based format
- Range scans · Doubly-linked leaf nodes enable efficient bidirectional iteration
- Immutable · Tree is bulk-loaded from sorted memtable data during flush and never modified afterward
- Compression · Nodes compress independently using the same algorithms (LZ4, LZ4-FAST, Zstd, Snappy)
- Large values · Values meeting or exceeding `klog_value_threshold` are stored in vlog, same as block-based format
- Bloom filter · Works identically -- checked before tree traversal to skip lookups for absent keys

**When to use B+tree format**
- Read-heavy workloads with frequent point lookups
- Workloads where read latency is more important than write throughput
- Large SSTables where block scanning becomes expensive

**Tradeoffs**
- Slightly higher write amplification during flush (building tree structure)
- Larger metadata overhead per node compared to block-based format
- Block-based format may be faster for sequential scans of entire SSTables

:::caution[Important]
`use_btree` **cannot be changed** after column family creation. Different column families can use different formats (some B+tree, some block-based). Both formats support the same compression algorithms and bloom filters.
:::

**Choosing a Compression Algorithm**

| Workload | Recommended Algorithm | Rationale |
|----------|----------------------|-----------|
| General purpose | `TDB_COMPRESS_LZ4` | Best balance of speed and compression |
| Write-heavy | `TDB_COMPRESS_LZ4_FAST` | Minimize CPU overhead on writes |
| Storage-constrained | `TDB_COMPRESS_ZSTD` | Maximum compression ratio |
| Read-heavy | `TDB_COMPRESS_ZSTD` | Reduce I/O bandwidth, decompression is fast |
| Pre-compressed data | `TDB_COMPRESS_NONE` | Avoid double compression overhead |
| CPU-constrained | `TDB_COMPRESS_NONE` or `TDB_COMPRESS_LZ4_FAST` | Minimize CPU usage |

### Updating Column Family Configuration

Update runtime-safe configuration settings. Configuration changes are applied to new operations only.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_column_family_config_t new_config = tidesdb_default_column_family_config();
new_config.write_buffer_size = 256 * 1024 * 1024;  
new_config.skip_list_max_level = 16;
new_config.skip_list_probability = 0.25f;
new_config.bloom_fpr = 0.001;       /* 0.1% false positive rate */
new_config.index_sample_ratio = 8;  /* sample 1 in 8 blocks */

int persist_to_disk = 1;            /* save to config.ini */
if (tidesdb_cf_update_runtime_config(cf, &new_config, persist_to_disk) == 0)
{
    printf("Configuration updated successfully\n");
}
```

**Updatable settings** (all applied by `tidesdb_cf_update_runtime_config`):
- `write_buffer_size` · Memtable flush threshold
- `skip_list_max_level` · Skip list level for **new** memtables
- `skip_list_probability` · Skip list probability for **new** memtables
- `bloom_fpr` · False positive rate for **new** SSTables
- `enable_bloom_filter` · Enable/disable bloom filters for **new** SSTables
- `enable_block_indexes` · Enable/disable block indexes for **new** SSTables
- `block_index_prefix_len` · Block index prefix length for **new** SSTables
- `index_sample_ratio` · Index sampling ratio for **new** SSTables
- `compression_algorithm` · Compression for **new** SSTables (existing SSTables retain their original compression)
- `klog_value_threshold` · Value log threshold for **new** writes
- `sync_mode` · Durability mode (TDB_SYNC_NONE, TDB_SYNC_INTERVAL, or TDB_SYNC_FULL). Also updates the active WAL's sync mode immediately.
- `sync_interval_us` · Sync interval in microseconds (only used when sync_mode is TDB_SYNC_INTERVAL)
- `level_size_ratio` · LSM level sizing (DCA recalculates capacities dynamically)
- `min_levels` · Minimum LSM levels
- `dividing_level_offset` · Compaction dividing level offset
- `l1_file_count_trigger` · L1 file count compaction trigger
- `l0_queue_stall_threshold` · Backpressure stall threshold
- `default_isolation_level` · Default transaction isolation level
- `min_disk_space` · Minimum disk space required
- `commit_hook_fn` / `commit_hook_ctx` · Commit hook callback and context

**Non-updatable settings** (not modified by this function):
- `comparator_name` · Cannot change sort order after creation (would corrupt key ordering in existing SSTables)
- `use_btree` · Cannot change klog format after creation (existing SSTables use the original format)

:::note[Backpressure Defaults]
The default `l0_queue_stall_threshold` is 20. The default `l1_file_count_trigger` is 4.
:::

**Configuration persistence**

If `persist_to_disk = 1`, changes are saved to `config.ini` in the column family directory. On restart, the configuration is loaded from this file.

```c
/* Save configuration to custom INI file */
tidesdb_cf_config_save_to_ini("custom_config.ini", "my_cf", &new_config);

/* Load configuration from INI file */
tidesdb_column_family_config_t loaded_config;
if (tidesdb_cf_config_load_from_ini("custom_config.ini", "my_cf", &loaded_config) == 0)
{
    printf("Configuration loaded successfully\n");
}
```

:::tip[Important Notes]
Changes apply immediately to new operations. Existing SSTables and memtables retain their original settings. The update operation is thread-safe.
:::

### Commit Hook (Change Data Capture)

`tidesdb_cf_set_commit_hook` registers an optional callback that fires synchronously after every transaction commit on a column family. The hook receives the full batch of committed operations atomically, enabling real-time change data capture without WAL parsing or external log consumers.

```c
int tidesdb_cf_set_commit_hook(tidesdb_column_family_t *cf,
                                tidesdb_commit_hook_fn fn,
                                void *ctx);
```

**Parameters**
| Name | Type | Description |
|------|------|-------------|
| `cf` | `tidesdb_column_family_t*` | Column family handle |
| `fn` | `tidesdb_commit_hook_fn` | Commit hook callback (or `NULL` to disable) |
| `ctx` | `void*` | User-provided context passed to the callback |

**Returns**
- `TDB_SUCCESS` on success
- `TDB_ERR_INVALID_ARGS` if `cf` is NULL

**Callback signature**

```c
typedef int (*tidesdb_commit_hook_fn)(const tidesdb_commit_op_t *ops, int num_ops,
                                      uint64_t commit_seq, void *ctx);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `ops` | `const tidesdb_commit_op_t*` | Array of committed operations (valid only during callback) |
| `num_ops` | `int` | Number of operations in the array |
| `commit_seq` | `uint64_t` | Monotonic commit sequence number |
| `ctx` | `void*` | User-provided context pointer |

The callback returns `0` on success. A non-zero return is logged as a warning but does not roll back the commit.

**Operation struct**

```c
typedef struct tidesdb_commit_op_t
{
    const uint8_t *key;
    size_t key_size;
    const uint8_t *value;      /* NULL for deletes */
    size_t value_size;         /* 0 for deletes */
    time_t ttl;
    int is_delete;             /* 1 for delete, 0 for put */
} tidesdb_commit_op_t;
```

**Example (replication sink)**
```c
typedef struct
{
    int socket_fd;
    uint8_t *send_buf;
    size_t buf_size;
} replication_ctx_t;

static int replication_hook(const tidesdb_commit_op_t *ops, int num_ops,
                            uint64_t commit_seq, void *ctx)
{
    replication_ctx_t *rctx = (replication_ctx_t *)ctx;
    for (int i = 0; i < num_ops; i++)
    {
        /* Serialize and send each op to replica */
        send_to_replica(rctx->socket_fd, commit_seq, &ops[i]);
    }
    return 0;
}

/* Attach hook at runtime */
replication_ctx_t rctx = { .socket_fd = replica_fd };
tidesdb_cf_set_commit_hook(cf, replication_hook, &rctx);

/* Normal writes now trigger the hook automatically */
tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);
tidesdb_txn_put(txn, cf, key, key_size, value, value_size, -1);
tidesdb_txn_commit(txn);  /* replication_hook fires here */
tidesdb_txn_free(txn);

/* Detach hook */
tidesdb_cf_set_commit_hook(cf, NULL, NULL);
```

**Setting hook via config at creation time**
```c
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
cf_config.commit_hook_fn = replication_hook;
cf_config.commit_hook_ctx = &rctx;

tidesdb_create_column_family(db, "replicated_cf", &cf_config);
```

**Behavior**
- The hook fires after WAL write, memtable apply, and commit status marking are complete — the data is fully durable before the callback runs
- Hook failure (non-zero return) is logged but does not affect the commit result
- Each column family has its own independent hook; a multi-CF transaction fires the hook once per CF with only that CF's operations
- `commit_seq` is monotonically increasing across commits and can be used as a replication cursor
- Pointers in `tidesdb_commit_op_t` are valid only during the callback invocation — copy any data you need to retain
- The hook executes synchronously on the committing thread; keep the callback fast to avoid stalling writers
- Setting the hook to `NULL` disables it immediately with no restart required

**Use cases**
- Replication · Ship committed batches to replicas in commit order
- Event streaming · Publish mutations to Kafka, NATS, or any message broker
- Secondary indexing · Maintain a reverse index or materialized view
- Audit logging · Record every mutation with key, value, TTL, and sequence number
- Debugging · Attach a temporary hook in production to inspect live writes

:::note[Runtime-Only]
The `commit_hook_fn` and `commit_hook_ctx` fields are not persisted to `config.ini`. After a database restart, hooks must be re-registered by the application. This is by design — function pointers cannot be serialized.
:::

## Transactions

All operations in TidesDB are done through transactions for ACID guarantees per column family.

### Basic Transaction

```c
/* Get column family pointer first */
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
if (tidesdb_txn_begin(db, &txn) != 0)
{
    return -1;
}

const uint8_t *key = (uint8_t *)"mykey";
const uint8_t *value = (uint8_t *)"myvalue";

if (tidesdb_txn_put(txn, cf, key, 5, value, 7, -1) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

if (tidesdb_txn_commit(txn) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

### With TTL (Time-to-Live)

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

const uint8_t *key = (uint8_t *)"temp_key";
const uint8_t *value = (uint8_t *)"temp_value";

/* TTL is Unix timestamp (seconds since epoch) -- absolute expiration time */
time_t ttl = time(NULL) + 60;  /* Expires 60 seconds from now */

/* Use -1 for no expiration */
tidesdb_txn_put(txn, cf, key, 8, value, 10, ttl);
tidesdb_txn_commit(txn);
tidesdb_txn_free(txn);
```

:::tip[TTL Examples]
```c
time_t ttl = -1;

time_t ttl = time(NULL) + (5 * 60);

time_t ttl = time(NULL) + (60 * 60);

time_t ttl = 1730592000;
```
:::

### Getting a Key-Value Pair

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

const uint8_t *key = (uint8_t *)"mykey";
uint8_t *value = NULL;
size_t value_size = 0;

if (tidesdb_txn_get(txn, cf, key, 5, &value, &value_size) == 0)
{
    /* Use value */
    printf("Value: %.*s\n", (int)value_size, value);
    free(value);
}

tidesdb_txn_free(txn);
```

### Deleting a Key-Value Pair

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

const uint8_t *key = (uint8_t *)"mykey";
tidesdb_txn_delete(txn, cf, key, 5);

tidesdb_txn_commit(txn);
tidesdb_txn_free(txn);
```

### Multi-Operation Transaction

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_txn_put(txn, cf, (uint8_t *)"key1", 4, (uint8_t *)"value1", 6, -1);
tidesdb_txn_put(txn, cf, (uint8_t *)"key2", 4, (uint8_t *)"value2", 6, -1);
tidesdb_txn_delete(txn, cf, (uint8_t *)"old_key", 7);

/* Commit atomically -- all or nothing */
if (tidesdb_txn_commit(txn) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

### Transaction Rollback

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_txn_put(txn, cf, (uint8_t *)"key", 3, (uint8_t *)"value", 5, -1);

tidesdb_txn_rollback(txn);
tidesdb_txn_free(txn);
```

### Transaction Reset

`tidesdb_txn_reset` resets a committed or aborted transaction for reuse with a new isolation level. This avoids the overhead of freeing and reallocating transaction resources in hot loops.

```c
int tidesdb_txn_reset(tidesdb_txn_t *txn, tidesdb_isolation_level_t isolation);
```

**Parameters**
| Name | Type | Description |
|------|------|-------------|
| `txn` | `tidesdb_txn_t*` | Transaction handle (must be committed or aborted) |
| `isolation` | `tidesdb_isolation_level_t` | New isolation level for the reset transaction |

**Returns**
- `TDB_SUCCESS` on success
- `TDB_ERR_INVALID_ARGS` if txn is NULL, still active (not committed/aborted), or isolation level is invalid

**Example**
```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_txn_put(txn, cf, (uint8_t *)"key1", 4, (uint8_t *)"value1", 6, -1);
tidesdb_txn_commit(txn);

tidesdb_txn_reset(txn, TDB_ISOLATION_READ_COMMITTED);

tidesdb_txn_put(txn, cf, (uint8_t *)"key2", 4, (uint8_t *)"value2", 6, -1);
tidesdb_txn_commit(txn);

tidesdb_txn_free(txn);
```

**Behavior**
- The transaction must be committed or aborted before reset; resetting an active transaction returns `TDB_ERR_INVALID_ARGS`
- Internal buffers (ops array, read set arrays, arena pointer array, column family array, savepoints array) are retained to avoid reallocation
- Per-operation key/value data, arena buffers, hash tables, and savepoint children are freed
- A fresh `txn_id` and `snapshot_seq` are assigned based on the new isolation level
- The isolation level can be changed on each reset (e.g., `READ_COMMITTED` → `REPEATABLE_READ`)
- If switching to an isolation level that requires read tracking (`REPEATABLE_READ` or higher), read set arrays are allocated automatically
- SERIALIZABLE transactions are correctly unregistered from and re-registered to the active transaction list

**When to use**
- Batch processing · Reuse a single transaction across many commit cycles in a loop
- Connection pooling · Reset a transaction for a new request without reallocation
- High-throughput ingestion · Reduce malloc/free overhead in tight write loops

:::tip[Reset vs Free + Begin]
For a single transaction, `tidesdb_txn_reset` is functionally equivalent to calling `tidesdb_txn_free` followed by `tidesdb_txn_begin_with_isolation`. The difference is performance: reset retains allocated buffers and avoids repeated allocation overhead. This matters most in loops that commit and restart thousands of transactions.
:::

### Savepoints

Savepoints allow partial rollback within a transaction. You can create named savepoints and rollback to them without aborting the entire transaction.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_txn_put(txn, cf, (uint8_t *)"key1", 4, (uint8_t *)"value1", 6, -1);

if (tidesdb_txn_savepoint(txn, "sp1") != 0)
{
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_put(txn, cf, (uint8_t *)"key2", 4, (uint8_t *)"value2", 6, -1);

if (tidesdb_txn_rollback_to_savepoint(txn, "sp1") != 0)
{
    tidesdb_txn_rollback(txn);
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_put(txn, cf, (uint8_t *)"key3", 4, (uint8_t *)"value3", 6, -1);

if (tidesdb_txn_commit(txn) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

**Savepoint API**
- `tidesdb_txn_savepoint(txn, "name")` · Create a savepoint
- `tidesdb_txn_rollback_to_savepoint(txn, "name")` · Rollback to savepoint
- `tidesdb_txn_release_savepoint(txn, "name")` · Release savepoint without rolling back

**Savepoint behavior**
- Savepoints capture the transaction state at a specific point
- Rolling back to a savepoint discards all operations after that savepoint
- Releasing a savepoint frees its resources without rolling back
- Multiple savepoints can be created with different names
- Creating a savepoint with an existing name updates that savepoint
- Savepoints are automatically freed when the transaction commits or rolls back
- Returns `TDB_ERR_NOT_FOUND` if the savepoint name doesn't exist

### Multi-Column-Family Transactions

TidesDB supports atomic transactions across multiple column families with true all-or-nothing semantics.

```c
tidesdb_column_family_t *users_cf = tidesdb_get_column_family(db, "users");
tidesdb_column_family_t *orders_cf = tidesdb_get_column_family(db, "orders");
if (!users_cf || !orders_cf) return -1;

tidesdb_txn_t *txn = NULL;
if (tidesdb_txn_begin(db, &txn) != 0)
{
    return -1;
}

tidesdb_txn_put(txn, users_cf, (uint8_t *)"user:1000", 9, 
                (uint8_t *)"John Doe", 8, -1);

tidesdb_txn_put(txn, orders_cf, (uint8_t *)"order:5000", 10,
                (uint8_t *)"user:1000|product:A", 19, -1);

if (tidesdb_txn_commit(txn) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

**Multi-CF guarantees**
- Either all CFs commit or none do (atomic)
- Automatically detected when operations span multiple CFs
- Uses global sequence numbers for atomic ordering
- Each CF's WAL receives operations with the same commit sequence number
- No two-phase commit or coordinator overhead

### Isolation Levels

TidesDB supports five MVCC isolation levels for fine-grained concurrency control.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;

tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_READ_UNCOMMITTED, &txn);
tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_READ_COMMITTED, &txn);
tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_REPEATABLE_READ, &txn);
tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_SNAPSHOT, &txn);
tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_SERIALIZABLE, &txn);

tidesdb_txn_put(txn, cf, (uint8_t *)"key", 3, (uint8_t *)"value", 5, -1);

int result = tidesdb_txn_commit(txn);
if (result == TDB_ERR_CONFLICT)
{
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_txn_free(txn);
```

**Isolation level characteristics**
- READ UNCOMMITTED · Maximum concurrency, minimal consistency
- READ COMMITTED · Balanced for OLTP workloads (default)
- REPEATABLE READ · Strong point read consistency
- SNAPSHOT · Prevents lost updates with write-write conflict detection
- SERIALIZABLE · Strongest guarantees with full SSI, higher abort rates

## Iterators

Iterators provide efficient forward and backward traversal over key-value pairs.

:::caution[Memory Ownership]
The key and value pointers returned by `tidesdb_iter_key()` and `tidesdb_iter_value()` are **internal pointers owned by the iterator**. Do **NOT** free them. They remain valid until the next `tidesdb_iter_next()`, `tidesdb_iter_prev()`, seek operation, or `tidesdb_iter_free()` call. If you need to retain the data beyond the current iteration step, copy it to your own buffer.
:::

### Forward Iteration

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_iter_t *iter = NULL;
if (tidesdb_iter_new(txn, cf, &iter) != 0)
{
    tidesdb_txn_free(txn);
    return -1;
}

tidesdb_iter_seek_to_first(iter);

while (tidesdb_iter_valid(iter))
{
    uint8_t *key = NULL;
    size_t key_size = 0;
    uint8_t *value = NULL;
    size_t value_size = 0;
    
    if (tidesdb_iter_key(iter, &key, &key_size) == 0 &&
        tidesdb_iter_value(iter, &value, &value_size) == 0)
    {
        printf("Key: %.*s, Value: %.*s\n", 
               (int)key_size, key, (int)value_size, value);
    }
    
    tidesdb_iter_next(iter);
}

tidesdb_iter_free(iter);
tidesdb_txn_free(txn);
```

### Backward Iteration

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_iter_t *iter = NULL;
tidesdb_iter_new(txn, cf, &iter);

tidesdb_iter_seek_to_last(iter);

while (tidesdb_iter_valid(iter))
{
    tidesdb_iter_prev(iter);
}

tidesdb_iter_free(iter);
tidesdb_txn_free(txn);
```

### Iterator Seek Operations

TidesDB provides seek operations that allow you to position an iterator at a specific key or key range without scanning from the beginning.

**How Seek Works**

**With Block Indexes Enabled** (`enable_block_indexes = 1`):
- Uses compact block index with parallel arrays (min/max key prefixes and file positions)
- Binary search through sampled keys at configurable ratio (default 1:1 via `index_sample_ratio`, meaning every block is indexed)
- Jumps directly to the target block using the file position
- Scans forward from that block to find the exact key
- **Performance** · O(log n) binary search + O(k) entries per block scan

Block indexes provide dramatic speedup for large SSTables at the cost of ~2-5% storage overhead for the compact index structure (parallel arrays with delta-encoded file positions).

#### Seek to Specific Key

**`tidesdb_iter_seek(iter, key, key_size)`** · Positions iterator at the first key >= target key

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

tidesdb_txn_t *txn = NULL;
tidesdb_txn_begin(db, &txn);

tidesdb_iter_t *iter = NULL;
tidesdb_iter_new(txn, cf, &iter);

/* Seek to specific key */
const char *target = "user:1000";
if (tidesdb_iter_seek(iter, (uint8_t *)target, strlen(target)) == 0)
{
    /* Iterator is now positioned at "user:1000" or the next key after it */
    if (tidesdb_iter_valid(iter))
    {
        uint8_t *key = NULL;
        size_t key_size = 0;
        tidesdb_iter_key(iter, &key, &key_size);
        printf("Found: %.*s\n", (int)key_size, key);
    }
}

tidesdb_iter_free(iter);
tidesdb_txn_free(txn);
```

**`tidesdb_iter_seek_for_prev(iter, key, key_size)`** · Positions iterator at the last key <= target key

```c
/* Seek for reverse iteration */
const char *target = "user:2000";
if (tidesdb_iter_seek_for_prev(iter, (uint8_t *)target, strlen(target)) == 0)
{
    /* Iterator is now positioned at "user:2000" or the previous key before it */
    while (tidesdb_iter_valid(iter))
    {
        /* Iterate backwards from this point */
        tidesdb_iter_prev(iter);
    }
}
```

#### Prefix Seeking

Since `tidesdb_iter_seek` positions the iterator at the first key >= target, you can use a prefix as the seek target to efficiently scan all keys sharing that prefix:

```c
/* Seek to prefix and iterate all matching keys */
const char *prefix = "user:";
if (tidesdb_iter_seek(iter, (uint8_t *)prefix, strlen(prefix) + 1) == 0)
{
    while (tidesdb_iter_valid(iter))
    {
        uint8_t *key = NULL;
        size_t key_size = 0;
        tidesdb_iter_key(iter, &key, &key_size);
        
        /* Stop when keys no longer match prefix */
        if (strncmp((char *)key, prefix, strlen(prefix)) != 0) break;
        
        /* Process key */
        printf("Found: %.*s\n", (int)key_size, key);
        
        if (tidesdb_iter_next(iter) != TDB_SUCCESS) break;
    }
}
```

This pattern works across both memtables and SSTables. When block indexes are enabled, the seek operation uses binary search to jump directly to the relevant block, making prefix scans efficient even on large datasets.

## Custom Comparators

TidesDB uses comparators to determine the sort order of keys throughout the entire system: memtables, SSTables, block indexes, and iterators all use the same comparison logic. Once a comparator is set for a column family, it **cannot be changed** without corrupting data.

### Built-in Comparators

TidesDB provides six built-in comparators that are automatically registered on database open:

**`"memcmp"` (default)** · Binary byte-by-byte comparison
- Compares min(key1_size, key2_size) bytes using `memcmp()`
- If bytes are equal, shorter key sorts first
- **Use case** · Binary keys, raw byte data, general purpose

**`"lexicographic"`** · Null-terminated string comparison
- Uses `strcmp()` for lexicographic ordering
- Ignores key_size parameters (assumes null-terminated)
- **Use case** · C strings, text keys
- **Warning** · Keys must be null-terminated or behavior is undefined

**`"uint64"`** · Unsigned 64-bit integer comparison
- Interprets 8-byte keys as uint64_t values
- Falls back to memcmp if key_size != 8
- **Use case** · Numeric IDs, timestamps, counters
- **Example** · `uint64_t id = 1000; tidesdb_txn_put(txn, cf, (uint8_t*)&id, 8, ...)`

**`"int64"`** · Signed 64-bit integer comparison
- Interprets 8-byte keys as int64_t values
- Falls back to memcmp if key_size != 8
- **Use case** · Signed numeric keys, relative timestamps
- **Example** · `int64_t offset = -500; tidesdb_txn_put(txn, cf, (uint8_t*)&offset, 8, ...)`

**`"reverse"`** · Reverse binary comparison
- Negates the result of memcmp comparator
- Sorts keys in descending order
- **Use case** · Reverse chronological order, descending IDs

**`"case_insensitive"`** · Case-insensitive ASCII comparison
- Converts A-Z to a-z during comparison
- Compares min(key1_size, key2_size) bytes
- If bytes are equal (ignoring case), shorter key sorts first
- **Use case** · Case-insensitive text keys, usernames, email addresses

### Custom Comparator Registration

```c
/* Define your comparison function */
int my_timestamp_compare(const uint8_t *key1, size_t key1_size,
                         const uint8_t *key2, size_t key2_size, void *ctx)
{
    (void)ctx;  /* unused */
    
    if (key1_size != 8 || key2_size != 8)
    {
        /* fallback for invalid sizes */
        return memcmp(key1, key2, key1_size < key2_size ? key1_size : key2_size);
    }
    
    uint64_t ts1, ts2;
    memcpy(&ts1, key1, 8);
    memcpy(&ts2, key2, 8);
    
    /* reverse order for newest-first */
    if (ts1 > ts2) return -1;
    if (ts1 < ts2) return 1;
    return 0;
}

/* Register before creating column families */
tidesdb_register_comparator(db, "timestamp_desc", my_timestamp_compare, NULL, NULL);

/* Use in column family */
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();
strncpy(cf_config.comparator_name, "timestamp_desc", TDB_MAX_COMPARATOR_NAME - 1);
cf_config.comparator_name[TDB_MAX_COMPARATOR_NAME - 1] = '\0';
tidesdb_create_column_family(db, "events", &cf_config);
```

### Retrieving a Registered Comparator

Use `tidesdb_get_comparator` to retrieve a previously registered comparator by name:

```c
tidesdb_comparator_fn fn = NULL;
void *ctx = NULL;

if (tidesdb_get_comparator(db, "timestamp_desc", &fn, &ctx) == 0)
{
    /* Comparator found - fn and ctx are now populated */
    printf("Comparator 'timestamp_desc' is registered\n");
}
else
{
    /* Comparator not found */
    printf("Comparator not registered\n");
}
```

**Use cases**
- Validation · Check if a comparator is registered before creating a column family
- Debugging · Verify comparator registration during development
- Dynamic configuration · Query available comparators at runtime

**Comparator function signature**
```c
int (*comparator_fn)(const uint8_t *key1, size_t key1_size,
                     const uint8_t *key2, size_t key2_size,
                     void *ctx);
```

**Return values**
- `< 0` if key1 < key2
- `0` if key1 == key2
- `> 0` if key1 > key2

:::caution[Important]
Comparators must be **registered before** creating column families that use them. Once set, a comparator **cannot be changed** for a column family. The same comparator is used across memtables, SSTables, block indexes, and iterators.
:::
- Custom comparators can use the `ctx` parameter for runtime configuration


## Sync Modes

Control durability vs performance tradeoff with three sync modes.

```c
tidesdb_column_family_config_t cf_config = tidesdb_default_column_family_config();

/* TDB_SYNC_NONE     -- Fastest, least durable (OS handles flushing) */
cf_config.sync_mode = TDB_SYNC_NONE;

/* TDB_SYNC_INTERVAL -- Balanced performance with periodic background syncing */
cf_config.sync_mode = TDB_SYNC_INTERVAL;
cf_config.sync_interval_us = 128000;  /* Sync every 128ms (default) */

/* TDB_SYNC_FULL     -- Most durable (fsync on every write) */
cf_config.sync_mode = TDB_SYNC_FULL;

tidesdb_create_column_family(db, "my_cf", &cf_config);
```

:::note[Sync Mode Options]
- **TDB_SYNC_NONE** · No explicit sync, relies on OS page cache (fastest, least durable)
    - Best for · Maximum throughput, acceptable data loss on crash
    - Use case · Caches, temporary data, reproducible workloads

- **TDB_SYNC_INTERVAL** · Periodic background syncing at configurable intervals (balanced)
    - Best for · Production workloads requiring good performance with bounded data loss
    - Use case · Most applications, configurable durability window
    - Features:
        - Single background sync thread monitors all column families using interval mode
        - Configurable sync interval via `sync_interval_us` (microseconds)
        - Structural operations (flush, compaction, WAL rotation) always enforce durability
        - At most `sync_interval_us` worth of data at risk on crash
        - Mid-durability correctness - When memtables rotate, the WAL receives an escalated fsync before entering the flush queue. Sorted run creation and merge operations always propagate full sync to block managers regardless of sync mode.

- **TDB_SYNC_FULL** · Fsync on every write operation (slowest, most durable)
    - Best for · Critical data requiring maximum durability
    - Use case · Financial transactions, audit logs, critical metadata
    - Note: Structural operations always use fsync regardless of sync mode
      :::

### Sync Interval Examples

```c
/* Sync every 100ms (good for low-latency requirements) */
cf_config.sync_mode = TDB_SYNC_INTERVAL;
cf_config.sync_interval_us = 100000;

/* Sync every 128ms (default) */
cf_config.sync_mode = TDB_SYNC_INTERVAL;
cf_config.sync_interval_us = 128000;

/* Sync every 1 second (higher throughput, more data at risk) */
cf_config.sync_mode = TDB_SYNC_INTERVAL;
cf_config.sync_interval_us = 1000000;
```

:::tip[Structural Operations]
Regardless of sync mode, TidesDB **always** enforces durability for structural operations:
- Memtable flush to SSTable
- SSTable compaction and merging
- WAL rotation
- Column family metadata updates

This ensures the database structure remains consistent even if user data syncing is delayed.
:::

## Compaction

TidesDB performs automatic background compaction when L1 reaches the configured file count trigger (default: 4 SSTables). However, you can manually trigger compaction for specific scenarios.

### Checking Flush/Compaction Status

Check if a column family currently has flush or compaction operations in progress.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

/* Check if flushing is in progress */
if (tidesdb_is_flushing(cf))
{
    printf("Flush in progress\n");
}

/* Check if compaction is in progress */
if (tidesdb_is_compacting(cf))
{
    printf("Compaction in progress\n");
}
```

**Use cases**
- Graceful shutdown · Wait for background operations to complete before closing
- Maintenance windows · Check if operations are running before triggering manual compaction
- Monitoring · Track background operation status for observability
- Testing · Verify flush/compaction behavior in unit tests

**Return values**
- `1` · Operation is in progress
- `0` · No operation in progress (or invalid column family)

### Manual Flush

Manually flush a column family's memtable to disk. This creates a new SSTable (sorted run) in level 1.

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

/* Trigger flush manually */
if (tidesdb_flush_memtable(cf) != 0)
{
    fprintf(stderr, "Failed to trigger flush\n");
    return -1;
}
```

**When to use manual flush**
- Before backup · Ensure all in-memory data is persisted before taking a backup
- Memory pressure · Force data to disk when memory usage is high
- Testing · Verify SSTable creation and compaction behavior
- Graceful shutdown · Flush pending data before closing the database

**Behavior**
- Enqueues flush work in the global flush thread pool
- Returns immediately (non-blocking) -- flush runs asynchronously in background threads
- If flush is already running for the column family, the call succeeds but doesn't queue duplicate work
- Thread-safe -- can be called concurrently from multiple threads

### Manual Compaction

```c
tidesdb_column_family_t *cf = tidesdb_get_column_family(db, "my_cf");
if (!cf) return -1;

/* Trigger compaction manually */
if (tidesdb_compact(cf) != 0)
{
    fprintf(stderr, "Failed to trigger compaction\n");
    return -1;
}
```

**When to use manual compaction**

- After bulk deletes · Reclaim disk space by removing tombstones and obsolete versions
- After bulk updates · Consolidate multiple versions of keys into single entries
- Before read-heavy workloads · Optimize read performance by reducing the number of levels to search
- During maintenance windows · Proactively compact during low-traffic periods to avoid compaction during peak load
- After TTL expiration · Remove expired entries to reclaim storage
- Space optimization · Force compaction to reduce space amplification when storage is constrained

**Behavior**

- Enqueues compaction work in the global compaction thread pool
- Returns immediately (non-blocking) -- compaction runs asynchronously in background threads
- If compaction is already running for the column family, the call succeeds but doesn't queue duplicate work
- Compaction merges SSTables across levels, removes tombstones, expired TTL entries, and obsolete versions
- Thread-safe -- can be called concurrently from multiple threads

**Performance considerations**

- Manual compaction uses the same thread pool as automatic background compaction
- Configure thread pool size via `config.num_compaction_threads` (default: 2)
- Compaction is I/O intensive -- avoid triggering during peak write workloads
- Multiple column families can compact in parallel up to the thread pool limit

See [How does TidesDB work?](/getting-started/how-does-tidesdb-work#6-compaction-policy) for details on compaction algorithms, merge strategies, and parallel compaction.

## Thread Pools

TidesDB uses separate thread pools for flush and compaction operations. Understanding the parallelism model is important for optimal configuration.

**Parallelism semantics**
- Cross-CF parallelism · Multiple flush/compaction workers CAN process different column families in parallel
- Within-CF serialization · A single column family can only have one flush and one compaction running at any time (enforced by atomic `is_flushing` and `is_compacting` flags)
- No intra-CF memtable parallelism · Even if a CF has multiple immutable memtables queued, they are flushed sequentially

**Thread pool sizing guidance**
- Single column family · Set `num_flush_threads = 1` and `num_compaction_threads = 1`. Additional threads provide no benefit since only one operation per CF can run at a time -- extra threads will simply wait idle.
- Multiple column families · Set thread counts up to the number of column families for maximum parallelism. With N column families and M workers (where M ≤ N), throughput scales linearly.

**Configuration**
```c
tidesdb_config_t config = {
    .db_path = "./mydb",
    .num_flush_threads = 2,                /* Flush thread pool size (default: 2) */
    .num_compaction_threads = 2,           /* Compaction thread pool size (default: 2) */
    .log_level = TDB_LOG_INFO,
    .block_cache_size = 64 * 1024 * 1024,  /* 64MB global block cache (default: 64MB) */
    .max_open_sstables = 256,              /* LRU cache for SSTable objects (default: 256, each has 2 FDs) */
};

tidesdb_t *db = NULL;
tidesdb_open(&config, &db);
```

:::note
`max_open_sstables` is a **storage-engine-level** configuration, not a column family configuration. It controls the LRU cache size for SSTable structures. Each SSTable uses 2 file descriptors (klog + vlog), so 256 SSTables = 512 file descriptors.
:::

See [How does TidesDB work?](/getting-started/how-does-tidesdb-work#75-thread-pool-architecture) for details on thread pool architecture and work distribution.

## Utility Functions

### tidesdb_free

Use `tidesdb_free` to free memory allocated by TidesDB. This is particularly useful for FFI/language bindings where the caller needs to free memory returned by TidesDB functions (e.g., values from `tidesdb_txn_get`).

```c
uint8_t *value = NULL;
size_t value_size = 0;

if (tidesdb_txn_get(txn, cf, key, key_size, &value, &value_size) == 0)
{
    /* Use value */
    printf("Value: %.*s\n", (int)value_size, value);
    
    /* Free using tidesdb_free (or standard free) */
    tidesdb_free(value);
}
```

:::note[When to use tidesdb_free]
- FFI bindings · Language bindings (Java, Rust, Go, Python) should use `tidesdb_free` to ensure memory is freed by the same allocator that allocated it
- Cross-platform · Ensures correct deallocation on all platforms
- Consistency · Provides a uniform API for memory management

For native C/C++ applications, `free()` works identically since TidesDB uses the standard allocator.
:::