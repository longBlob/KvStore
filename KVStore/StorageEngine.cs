namespace KvStore;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
///  A bitcask inspired key-value append only store
/// </summary>
public class StorageEngine : IDisposable
{
    private readonly string _dataDirectory;
    private readonly bool _synchronousWrites;
    private readonly int _writeShardCount;
    private readonly long _maxSegmentBytes;
    private readonly int _maxKeySize = 1 * 1024 * 1024;
    private readonly int _maxValueSize = 256 * 1024 * 1024;

    private readonly string _manifestFileName = "segments.manifest";
    private readonly string _counterFile;
    private readonly string _indexCheckpointFile;

    private readonly ConcurrentDictionary<string, IndexEntry> _index = new();
    private readonly FileStream[] _activeWriteStreams;
    private readonly object[] _writeLocks;
    private readonly long[] _bytesWrittenSinceRotatePerShard;
    private readonly bool[] _compactionRunningPerShard;

    private readonly List<string> _segments = new();
    private readonly BlockingCollection<WriteRequest>[] _shardQueues;
    private readonly Task[] _shardFlushTasks;
    private readonly CancellationTokenSource _cts = new();

    private readonly ConcurrentDictionary<string, MemoryMappedFile> _mmfCache = new();
    private long _nextSegmentCounter;

    private readonly TimeSpan _snapshotInterval = TimeSpan.FromMinutes(1);
    private readonly TimeSpan _compactionInterval = TimeSpan.FromMinutes(5);
    private readonly TimeSpan _indexCheckpointInterval = TimeSpan.FromMinutes(1);

    private Func<ReplicationRecord, Task>? _replicationCallback;

    public StorageEngine(string dataDirectory, bool synchronousWrites = false, int writeShardCount = 32,
        long? maxSegmentBytes = null)
    {
        _dataDirectory = dataDirectory ?? throw new ArgumentNullException(nameof(dataDirectory));
        Directory.CreateDirectory(_dataDirectory);

        _synchronousWrites = synchronousWrites;
        _writeShardCount = Math.Max(1, writeShardCount);
        _maxSegmentBytes = maxSegmentBytes ?? 64L * 1024 * 1024;

        _counterFile = Path.Combine(_dataDirectory, "segment_counter.txt");
        _indexCheckpointFile = Path.Combine(_dataDirectory, "index.checkpoint");
        _nextSegmentCounter = LoadNextSegmentCounter();

        _activeWriteStreams = new FileStream[_writeShardCount];
        _writeLocks = new object[_writeShardCount];
        _bytesWrittenSinceRotatePerShard = new long[_writeShardCount];
        _compactionRunningPerShard = new bool[_writeShardCount];

        _shardQueues = new BlockingCollection<WriteRequest>[_writeShardCount];
        _shardFlushTasks = new Task[_writeShardCount];

        for (int i = 0; i < _writeShardCount; i++)
        {
            _writeLocks[i] = new object();
            long segId = GetNextSegmentId();
            _activeWriteStreams[i] = OpenSegmentWrite(segId, append: false);
            _segments.Add(GetSegmentPath(segId));
            _bytesWrittenSinceRotatePerShard[i] = 0;
        }

        RecoverSegmentsAndIndex();
        InitializeShardQueues();
        StartSnapshotLoop();
        StartIndexCheckpointLoop();
        StartShardCompactionLoop();
    }

    public void SetReplicationCallback(Func<ReplicationRecord, Task> callback)
    {
        _replicationCallback = callback;
    }

    #region Public API

    public async Task PutAsync(string key, byte[] value)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (value == null) throw new ArgumentNullException(nameof(value));
        if (Encoding.UTF8.GetByteCount(key) > _maxKeySize) throw new ArgumentException("Key too large");
        if (value.Length > _maxValueSize) throw new ArgumentException("Value too large");

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var req = new WriteRequest(key, value, false, tcs);

        if (!EnqueueWrite(req)) tcs.SetException(new InvalidOperationException("Shard queue full"));

        await tcs.Task;
        if (_replicationCallback != null) await _replicationCallback(ReplicationRecord.Put(key, value));
    }

    public async Task BatchPutAsync(IList<string> keys, IList<byte[]> values)
    {
        if (keys == null) throw new ArgumentNullException(nameof(keys));
        if (values == null) throw new ArgumentNullException(nameof(values));
        if (keys.Count != values.Count) throw new ArgumentException("keys and values must have same length");

        var tcsList = new TaskCompletionSource<bool>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
        {
            var key = keys[i];
            var value = values[i];

            if (Encoding.UTF8.GetByteCount(key) > _maxKeySize) throw new ArgumentException($"Key too large: {key}");
            if (value.Length > _maxValueSize) throw new ArgumentException($"Value too large for key: {key}");

            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            tcsList[i] = tcs;

            var req = new WriteRequest(key, value, false, tcs);
            if (!EnqueueWrite(req)) tcs.SetException(new InvalidOperationException($"Shard queue full for key: {key}"));
        }

        await Task.WhenAll(tcsList.Select(t => t.Task));

        if (_replicationCallback != null)
        {
            var replicationTasks = new List<Task>();
            for (int i = 0; i < keys.Count; i++)
                replicationTasks.Add(_replicationCallback(ReplicationRecord.Put(keys[i], values[i])));
            await Task.WhenAll(replicationTasks);
        }
    }

    public async Task DeleteAsync(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var req = new WriteRequest(key, Array.Empty<byte>(), true, tcs);

        if (!EnqueueWrite(req)) tcs.SetException(new InvalidOperationException("Shard queue full"));

        await tcs.Task;
        if (_replicationCallback != null) await _replicationCallback(ReplicationRecord.Del(key));
    }

    public byte[]? Read(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (!_index.TryGetValue(key, out var entry) || entry.Deleted) return null;

        string path = GetSegmentPath(entry.SegmentId);
        if (!File.Exists(path))
        {
            _index.TryRemove(key, out _);
            return null;
        }

        var fi = new FileInfo(path);
        if (entry.ValuePosition + entry.Length > fi.Length)
        {
            _index.TryRemove(key, out _);
            return null;
        }

        try
        {
            using var accessor = GetOrCreateMmf(path)
                .CreateViewAccessor(entry.ValuePosition, entry.Length, MemoryMappedFileAccess.Read);
            byte[] buffer = new byte[entry.Length];
            accessor.ReadArray(0, buffer, 0, buffer.Length);
            return buffer;
        }
        catch
        {
            try
            {
                using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                fs.Position = entry.ValuePosition;
                byte[] buffer = new byte[entry.Length];
                int read = 0;
                while (read < buffer.Length)
                {
                    int r = fs.Read(buffer, read, buffer.Length - read);
                    if (r <= 0) break;
                    read += r;
                }

                if (read != buffer.Length) return null;
                return buffer;
            }
            catch
            {
                _index.TryRemove(key, out _);
                return null;
            }
        }
    }

    public IEnumerable<byte[]?> ReadKeyRange(string startKey, string endKey)
    {
        if (startKey == null) throw new ArgumentNullException(nameof(startKey));
        if (endKey == null) throw new ArgumentNullException(nameof(endKey));
        if (string.Compare(startKey, endKey, StringComparison.Ordinal) > 0) yield break;

        foreach (var kv in _index)
        {
            if (!kv.Value.Deleted &&
                string.Compare(kv.Key, startKey, StringComparison.Ordinal) >= 0 &&
                string.Compare(kv.Key, endKey, StringComparison.Ordinal) <= 0)
            {
                // Directly yield raw bytes from Read
                var value = Read(kv.Key);
                if (value != null)
                    
                    yield return value;
            }
        }
    }


    #endregion

    #region Sharded Async Write Pipeline

    private record WriteRequest(string Key, byte[] Value, bool IsDeleted, TaskCompletionSource<bool> CompletionSource);

    private void InitializeShardQueues()
    {
        for (int shard = 0; shard < _writeShardCount; shard++)
        {
            _shardQueues[shard] = new BlockingCollection<WriteRequest>(50_000);
            int localShard = shard;
            _shardFlushTasks[localShard] = Task.Run(() => ShardFlushLoopAsync(localShard));
        }
    }

    private bool EnqueueWrite(WriteRequest req) => _shardQueues[GetShardIndex(req.Key)].TryAdd(req, 0);

    private async Task ShardFlushLoopAsync(int shard)
    {
        var batch = new List<WriteRequest>();
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                if (!_shardQueues[shard].TryTake(out var req, 50))
                {
                    if (batch.Count > 0) FlushShardBatch(shard, batch);
                    batch.Clear();
                    await Task.Delay(10, _cts.Token).ContinueWith(_ => { });
                    continue;
                }

                batch.Add(req);
                while (batch.Count < 64 && _shardQueues[shard].TryTake(out var more, 0)) batch.Add(more);

                FlushShardBatch(shard, batch);
                batch.Clear();
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex )
            {
                Console.WriteLine(ex);
            }
        }
    }

    private void FlushShardBatch(int shard, List<WriteRequest> batch)
    {
        var stream = _activeWriteStreams[shard];
        lock (_writeLocks[shard])
        {
            long segmentId = GetActiveSegmentId(shard);

            foreach (var req in batch)
            {
                byte[] keyBytes = Encoding.UTF8.GetBytes(req.Key);
                int valueLen = req.IsDeleted ? 0 : req.Value.Length;
                long recordStart = stream.Position;

                using var ms = new MemoryStream();
                using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
                {
                    bw.Write(keyBytes.Length);
                    bw.Write(keyBytes);
                    bw.Write(valueLen);
                    bw.Write(req.IsDeleted ? (byte)1 : (byte)0);
                    if (!req.IsDeleted) bw.Write(req.Value);
                    bw.Flush();

                    byte[] blob = ms.ToArray();
                    uint crc = Crc32C.Compute(blob);

                    stream.Write(BitConverter.GetBytes(blob.Length));
                    stream.Write(blob);
                    stream.Write(BitConverter.GetBytes(crc));

                    long valuePos = recordStart + 4 + 4 + keyBytes.Length + 4 + 1;

                    if (!req.IsDeleted)
                        _index[req.Key] = new IndexEntry(segmentId, valuePos, valueLen, false);
                    else
                        _index.TryRemove(req.Key, out _);

                    req.CompletionSource.SetResult(true);
                    _bytesWrittenSinceRotatePerShard[shard] += 4 + blob.Length + 4;

                    if (_bytesWrittenSinceRotatePerShard[shard] >= _maxSegmentBytes)
                        RotateActiveSegmentShard(shard);
                }
            }

            stream.Flush(_synchronousWrites);
        }
    }

    #endregion

    #region Segment Management & Compaction

    private long GetActiveSegmentId(int shard) => ParseSegmentName(Path.GetFileName(_activeWriteStreams[shard].Name));

    private void RotateActiveSegmentShard(int shard)
    {
        lock (_writeLocks[shard])
        {
            try
            {
                _activeWriteStreams[shard].Flush(true);
            }
            catch (Exception ex )
            {
                Console.WriteLine(ex);
            }

            _activeWriteStreams[shard].Dispose();

            long segId = GetNextSegmentId();
            _activeWriteStreams[shard] = OpenSegmentWrite(segId, append: false);
            _segments.Add(GetSegmentPath(segId));
            _bytesWrittenSinceRotatePerShard[shard] = 0;

            SaveManifest();
        }
    }

    private void SaveManifest()
    {
        try
        {
            var manifestPath = Path.Combine(_dataDirectory, _manifestFileName);
            var tmp = manifestPath + ".tmp";
            File.WriteAllLines(tmp, _segments);
            if (File.Exists(manifestPath)) File.Replace(tmp, manifestPath, null);
            else File.Move(tmp, manifestPath);
        }
        catch (Exception ex )
        {
            Console.WriteLine(ex);
        }
    }

    private void StartShardCompactionLoop()
    {
        for (int shard = 0; shard < _writeShardCount; shard++)
        {
            int localShard = shard;
            Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(_compactionInterval, _cts.Token);
                        await RunShardCompactionAsync(localShard);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception ex )
                    {
                        Console.WriteLine(ex);
                    }
                }
            });
        }
    }

    private async Task RunShardCompactionAsync(int shard)
    {
        lock (_writeLocks[shard])
        {
            if (_compactionRunningPerShard[shard]) return;
            _compactionRunningPerShard[shard] = true;
        }

        try
        {
            var shardSegments = _segments.Where(s => GetShardIndexFromSegment(s) == shard).ToList();
            if (!shardSegments.Any()) return;

            long newSegId = GetNextSegmentId();
            string newSegPath = GetSegmentPath(newSegId);

            await using var newFs = new FileStream(newSegPath, FileMode.Create, FileAccess.Write, FileShare.Read);
            long bytesWritten = 0;
            var compactedIndex = new Dictionary<string, IndexEntry>();

            foreach (var segPath in shardSegments)
            {
                await using var fs = new FileStream(segPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var br = new BinaryReader(fs, Encoding.UTF8);

                while (fs.Position < fs.Length)
                {
                    if (!TryReadInt32(br, fs, out int blobLen)) break;
                    if (fs.Length - fs.Position < blobLen + 4) break;

                    byte[] blob = br.ReadBytes(blobLen);
                    uint crcOnDisk = br.ReadUInt32();
                    if (Crc32C.Compute(blob) != crcOnDisk) break;

                    using var ms = new MemoryStream(blob);
                    using var br2 = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);

                    int keyLen = br2.ReadInt32();
                    string key = Encoding.UTF8.GetString(br2.ReadBytes(keyLen));
                    int valueLen = br2.ReadInt32();
                    bool isDeleted = br2.ReadByte() == 1;

                    if (!isDeleted)
                    {
                        long valuePos = bytesWritten + 4 + 4 + keyLen + 4 + 1;
                        compactedIndex[key] = new IndexEntry(newSegId, valuePos, valueLen, false);

                        newFs.Write(BitConverter.GetBytes(blob.Length));
                        newFs.Write(blob);
                        newFs.Write(BitConverter.GetBytes(Crc32C.Compute(blob)));
                        bytesWritten += 4 + blob.Length + 4;
                    }
                }
            }

            await newFs.FlushAsync();

            lock (_writeLocks[shard])
            {
                foreach (var oldSeg in shardSegments)
                {
                    _segments.Remove(oldSeg);
                    try
                    {
                        File.Delete(oldSeg);
                        if (_mmfCache.TryRemove(oldSeg, out var mmf)) mmf.Dispose();
                    }
                    catch (Exception ex )
                    {
                        Console.WriteLine(ex);
                    }
                }

                _segments.Add(newSegPath);
                foreach (var kv in compactedIndex) _index[kv.Key] = kv.Value;
                SaveManifest();
            }
        }
        finally
        {
            lock (_writeLocks[shard])
            {
                _compactionRunningPerShard[shard] = false;
            }
        }
    }

    #endregion
    
    private void StartSnapshotLoop()
    {
        Task.Run(async () =>
        {
            while (!_cts.IsCancellationRequested)
            {
                await Task.Delay(_snapshotInterval, _cts.Token).ContinueWith(_ => { });
                SaveManifest();
            }
        });
    }

    private void StartIndexCheckpointLoop()
    {
        Task.Run(async () =>
        {
            while (!_cts.IsCancellationRequested)
            {
                await Task.Delay(_indexCheckpointInterval, _cts.Token).ContinueWith(_ => { });
                SaveIndexCheckpoint();
            }
        });
    }

    private void SaveIndexCheckpoint()
    {
        try
        {
            var tmp = _indexCheckpointFile + ".tmp";
            using var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None);
            using var bw = new BinaryWriter(fs, Encoding.UTF8);

            foreach (var kv in _index)
            {
                var keyBytes = Encoding.UTF8.GetBytes(kv.Key);
                bw.Write(keyBytes.Length);
                bw.Write(keyBytes);
                bw.Write(kv.Value.SegmentId);
                bw.Write(kv.Value.ValuePosition);
                bw.Write(kv.Value.Length);
                bw.Write(kv.Value.Deleted);
            }

            bw.Flush();
            fs.Flush(true);

            if (File.Exists(_indexCheckpointFile))
                File.Replace(tmp, _indexCheckpointFile, null);
            else
                File.Move(tmp, _indexCheckpointFile);
        }
        catch (Exception ex )
        {
            Console.WriteLine(ex);
        }
    }

    private void LoadIndexCheckpoint()
    {
        if (!File.Exists(_indexCheckpointFile)) return;

        try
        {
            using var fs = new FileStream(_indexCheckpointFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var br = new BinaryReader(fs, Encoding.UTF8);

            while (fs.Position < fs.Length)
            {
                int keyLen = br.ReadInt32();
                var keyBytes = br.ReadBytes(keyLen);
                string key = Encoding.UTF8.GetString(keyBytes);

                long segId = br.ReadInt64();
                long pos = br.ReadInt64();
                int len = br.ReadInt32();
                bool deleted = br.ReadBoolean();

                string segPath = GetSegmentPath(segId);
                if (!File.Exists(segPath)) continue;
                var fi = new FileInfo(segPath);
                if (pos + len > fi.Length) continue;

                _index[key] = new IndexEntry(segId, pos, len, deleted);
            }
        }
        catch (Exception ex )
        {
            Console.WriteLine(ex);
        }
    }
    
    private long GetNextSegmentId()
    {
        long id = DateTime.UtcNow.Ticks + Interlocked.Increment(ref _nextSegmentCounter);
        PersistNextSegmentCounter();
        return id;
    }

    private long LoadNextSegmentCounter()
    {
        try
        {
            if (
                File.Exists(_counterFile)
            )
                return long.Parse(File.ReadAllText(_counterFile));
        }
        catch (Exception ex )
        {
            Console.WriteLine(ex);
        }

        return 0;
    }

    private void PersistNextSegmentCounter()
    {
        try
        {
            File.WriteAllText(_counterFile + ".tmp", _nextSegmentCounter.ToString());
            if (File.Exists(_counterFile)) File.Replace(_counterFile + ".tmp", _counterFile, null);
            else File.Move(_counterFile + ".tmp", _counterFile);
        }
        catch (Exception ex )
        {
            Console.WriteLine(ex);
        }
    }

    private string GetSegmentPath(long segmentId) => Path.Combine(_dataDirectory, $"seg_{segmentId}.dat");

    private FileStream OpenSegmentWrite(long segmentId, bool append) => new(GetSegmentPath(segmentId),
        append ? FileMode.Append : FileMode.Create, FileAccess.Write, FileShare.Read);

    private int GetShardIndex(string key) => (int)((uint)key.GetHashCode() % _writeShardCount);

    private int GetShardIndexFromSegment(string segmentPath) =>
        (int)(ParseSegmentName(Path.GetFileName(segmentPath)) % _writeShardCount);

    private MemoryMappedFile GetOrCreateMmf(string path) => _mmfCache.GetOrAdd(path,
        p => MemoryMappedFile.CreateFromFile(p, FileMode.Open, null, 0, MemoryMappedFileAccess.Read));

    private static long ParseSegmentName(string fileName)
    {
        fileName = Path.GetFileName(fileName);
        if (!fileName.StartsWith("seg_")) throw new FormatException($"Invalid segment file name: {fileName}");
        string withoutExt = Path.GetFileNameWithoutExtension(fileName);
        string numPart = withoutExt.Substring(4);
        return long.Parse(numPart);
    }

    private static bool TryReadInt32(
        BinaryReader br, 
        FileStream fs, 
        out int value)
    {
        if (fs.Length - fs.Position < 4)
        {
            value = default;
            return false;
        }

        value = br.ReadInt32();
        return true;
    }
    
    private void RecoverSegmentsAndIndex()
    {
        LoadIndexCheckpoint();

        var manifestPath = Path.Combine(_dataDirectory, _manifestFileName);
        List<string> segmentFiles = File.Exists(manifestPath)
            ? File.ReadAllLines(manifestPath).Where(l => !string.IsNullOrWhiteSpace(l)).ToList()
            : Directory.EnumerateFiles(_dataDirectory, "seg_*.dat").ToList();

        var shardGroups = segmentFiles.GroupBy(GetShardIndexFromSegment);

        Parallel.ForEach(shardGroups, shardGroup =>
        {
            foreach (var path in shardGroup.OrderBy(p => ParseSegmentName(Path.GetFileName(p))))
                ReplayAndRecoverSegment(path);
        });
    }

    private void ReplayAndRecoverSegment(string path)
    {
        long segId = ParseSegmentName(Path.GetFileName(path));
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var br = new BinaryReader(fs, Encoding.UTF8, leaveOpen: true);

        long lastGoodOffset = 0;
        while (fs.Position < fs.Length)
        {
            long recordStart = fs.Position;
            if (!TryReadInt32(br, fs, out int blobLen)) break;
            if (fs.Length - fs.Position < blobLen + 4) break;

            byte[] blob = br.ReadBytes(blobLen);
            uint crcOnDisk = br.ReadUInt32();
            if (Crc32C.Compute(blob) != crcOnDisk) break;

            using var ms = new MemoryStream(blob);
            using var br2 = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);

            int keyLen = br2.ReadInt32();
            string key = Encoding.UTF8.GetString(br2.ReadBytes(keyLen));

            int valueLen = br2.ReadInt32();
            bool isDeleted = br2.ReadByte() == 1;
            long valuePos = recordStart + 4 + 4 + keyLen + 4 + 1;

            lastGoodOffset = fs.Position;

            if (!isDeleted) _index[key] = new IndexEntry(segId, valuePos, valueLen, false);
            else _index.TryRemove(key, out _);
        }

        if (lastGoodOffset < fs.Length)
        {
            fs.SetLength(lastGoodOffset);
            fs.Flush(true);
        }
    }
    
    public void Dispose()
    {
        _cts.Cancel();
        foreach (var queue in _shardQueues) queue.CompleteAdding();
        Task.WaitAll(_shardFlushTasks.Where(t => t != null).ToArray());

        for (int i = 0; i < _writeShardCount; i++)
        {
            lock (_writeLocks[i])
            {
                try
                {
                    _activeWriteStreams[i].Flush(true);
                }
                catch (Exception ex )
                {
                    Console.WriteLine(ex);
                }

                try
                {
                    _activeWriteStreams[i].Dispose();
                }
                catch (Exception ex )
                {
                    Console.WriteLine(ex);
                }
            }
        }

        foreach (var mmf in _mmfCache.Values) mmf.Dispose();

        SaveIndexCheckpoint();
        SaveManifest();
    }
    

    private record IndexEntry(
        long SegmentId, 
        long ValuePosition, 
        int Length, 
        bool Deleted);

    private static class Crc32C
    {
        private static readonly uint[] Table = CreateTable();

        private static uint[] CreateTable()
        {
            const uint poly = 0x82F63B78u;
            var table = new uint[256];
            for (uint i = 0; i < 256; ++i)
            {
                uint crc = i;
                for (int j = 0; j < 8; ++j) crc = (crc >> 1) ^ ((crc & 1) != 0 ? poly : 0);
                table[i] = crc;
            }

            return table;
        }

        public static uint Compute(byte[] data)
        {
            uint crc = 0xFFFFFFFFu;
            foreach (var b in data) crc = (crc >> 8) ^ Table[(crc ^ b) & 0xFF];
            return ~crc;
        }
    }
    
}