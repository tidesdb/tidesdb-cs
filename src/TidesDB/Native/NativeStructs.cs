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
    public nint LevelKeyCounts;
    public nint Config;
    public ulong TotalKeys;
    public ulong TotalDataSize;
    public double AvgKeySize;
    public double AvgValueSize;
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
