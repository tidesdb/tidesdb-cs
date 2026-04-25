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

using System.Runtime.InteropServices;

namespace TidesDB.Native;

[StructLayout(LayoutKind.Sequential)]
internal struct NativeConfig
{
    public nint DbPath;
    public int NumFlushThreads;
    public int NumCompactionThreads;
    public int LogLevel;
    public nuint BlockCacheSize;
    public nuint MaxOpenSstables;
    public int LogToFile;
    public nuint LogTruncationAt;
    public nuint MaxMemoryUsage;
    public int UnifiedMemtable;
    public nuint UnifiedMemtableWriteBufferSize;
    public int UnifiedMemtableSkipListMaxLevel;
    public float UnifiedMemtableSkipListProbability;
    public int UnifiedMemtableSyncMode;
    public ulong UnifiedMemtableSyncIntervalUs;
    public nint ObjectStore;
    public nint ObjectStoreConfig;
}

[StructLayout(LayoutKind.Sequential)]
internal struct NativeObjStoreConfig
{
    public nint LocalCachePath;
    public nuint LocalCacheMaxBytes;
    public int CacheOnRead;
    public int CacheOnWrite;
    public int MaxConcurrentUploads;
    public int MaxConcurrentDownloads;
    public nuint MultipartThreshold;
    public nuint MultipartPartSize;
    public int SyncManifestToObject;
    public int ReplicateWal;
    public int WalUploadSync;
    public nuint WalSyncThresholdBytes;
    public int WalSyncOnCommit;
    public int ReplicaMode;
    public ulong ReplicaSyncIntervalUs;
    public int ReplicaReplayWal;
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeColumnFamilyConfig
{
    public fixed byte Name[128];
    public nuint WriteBufferSize;
    public nuint LevelSizeRatio;
    public int MinLevels;
    public int DividingLevelOffset;
    public nuint KlogValueThreshold;
    public int CompressionAlgo;
    public int EnableBloomFilter;
    public double BloomFpr;
    public int EnableBlockIndexes;
    public int IndexSampleRatio;
    public int BlockIndexPrefixLen;
    public int SyncMode;
    public ulong SyncIntervalUs;
    public fixed byte ComparatorName[64];
    public fixed byte ComparatorCtxStr[256];
    public nint ComparatorFnCached;
    public nint ComparatorCtxCached;
    public int SkipListMaxLevel;
    public float SkipListProbability;
    public int DefaultIsolationLevel;
    public ulong MinDiskSpace;
    public int L1FileCountTrigger;
    public int L0QueueStallThreshold;
    public int UseBtree;
    public nint CommitHookFn;
    public nint CommitHookCtx;
    // reserved field in db.h, retained for struct alignment with the C ABI; always 0
    public nuint ObjectTargetFileSize;
    public int ObjectLazyCompaction;
    public int ObjectPrefetchCompaction;
}

[StructLayout(LayoutKind.Sequential)]
internal struct NativeCommitOp
{
    public nint Key;
    public nuint KeySize;
    public nint Value;
    public nuint ValueSize;
    public long Ttl;
    public int IsDelete;
}

[StructLayout(LayoutKind.Sequential)]
internal struct NativeStats
{
    public int NumLevels;
    public nuint MemtableSize;
    public nint LevelSizes;
    public nint LevelNumSstables;
    public nint Config;
    public ulong TotalKeys;
    public ulong TotalDataSize;
    public double AvgKeySize;
    public double AvgValueSize;
    public nint LevelKeyCounts;
    public double ReadAmp;
    public double HitRate;
    public int UseBtree;
    public ulong BtreeTotalNodes;
    public uint BtreeMaxHeight;
    public double BtreeAvgHeight;
}

[StructLayout(LayoutKind.Sequential)]
internal struct NativeCacheStats
{
    public int Enabled;
    public nuint TotalEntries;
    public nuint TotalBytes;
    public ulong Hits;
    public ulong Misses;
    public double HitRate;
    public nuint NumPartitions;
}

[StructLayout(LayoutKind.Sequential)]
internal struct NativeDbStats
{
    public int NumColumnFamilies;
    public ulong TotalMemory;
    public ulong AvailableMemory;
    public nuint ResolvedMemoryLimit;
    public int MemoryPressureLevel;
    public int FlushPendingCount;
    public long TotalMemtableBytes;
    public int TotalImmutableCount;
    public int TotalSstableCount;
    public ulong TotalDataSizeBytes;
    public int NumOpenSstables;
    public ulong GlobalSeq;
    public long TxnMemoryBytes;
    public nuint CompactionQueueSize;
    public nuint FlushQueueSize;
    public int UnifiedMemtableEnabled;
    public long UnifiedMemtableBytes;
    public int UnifiedImmutableCount;
    public int UnifiedIsFlushing;
    public uint UnifiedNextCfIndex;
    public ulong UnifiedWalGeneration;
    public int ObjectStoreEnabled;
    public nint ObjectStoreConnector;
    public nuint LocalCacheBytesUsed;
    public nuint LocalCacheBytesMax;
    public int LocalCacheNumFiles;
    public ulong LastUploadedGeneration;
    public nuint UploadQueueDepth;
    public ulong TotalUploads;
    public ulong TotalUploadFailures;
    public int ReplicaMode;
}
