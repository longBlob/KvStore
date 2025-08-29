using System.Diagnostics;
using System.Text;
using KvStore;

namespace KvStoreTest
{
    public class StorageEngineTests : IDisposable
    {
        private readonly string _tempDir;
        private  StorageEngine _engine;

        public StorageEngineTests()
        {
            _tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _engine = new StorageEngine(_tempDir, synchronousWrites: true, writeShardCount: 2, maxSegmentBytes: 1024 * 1024);
        }

        public void Dispose()
        {
            _engine.Dispose();
            try
            {
                Directory.Delete(_tempDir, recursive: true);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        [Fact]
        public async Task Put_And_Read_Works()
        {
            var key = "user:1";
            var value = Encoding.UTF8.GetBytes("Alice");

            await _engine.PutAsync(key, value);
            var result = _engine.Read(key);

            Assert.NotNull(result);
            Assert.Equal("Alice", Encoding.UTF8.GetString(result));
        }

        [Fact]
        public async Task BatchPut_And_Read_Works()
        {
            var keys = new[] { "k1", "k2", "k3" };
            var values = keys.Select(k => Encoding.UTF8.GetBytes($"val-{k}")).ToList();

            await _engine.BatchPutAsync(keys, values);

            foreach (var k in keys)
            {
                var v = _engine.Read(k);
                Assert.NotNull(v);
                Assert.Equal($"val-{k}", Encoding.UTF8.GetString(v));
            }
        }

        [Fact]
        public async Task Delete_Removes_Key()
        {
            var key = "delete-me";
            await _engine.PutAsync(key, Encoding.UTF8.GetBytes("temp"));
            Assert.NotNull(_engine.Read(key));

            await _engine.DeleteAsync(key);

            Assert.Null(_engine.Read(key));
        }

        [Fact]
        public async Task ReadKeyRange_Returns_Expected_Values()
        {
            await _engine.PutAsync("a", Encoding.UTF8.GetBytes("1"));
            await _engine.PutAsync("b", Encoding.UTF8.GetBytes("2"));
            await _engine.PutAsync("c", Encoding.UTF8.GetBytes("3"));

            var values = _engine.ReadKeyRange("a", "b")
                .Select(v => Encoding.UTF8.GetString(v!)) 
                .ToList();

            Assert.Contains("1", values);
            Assert.Contains("2", values); 
            Assert.DoesNotContain("3", values); 
        }

        [Fact]
        public async Task Overwrite_Key_Replaces_Value()
        {
            var key = "overwrite";
            await _engine.PutAsync(key, Encoding.UTF8.GetBytes("first"));
            await _engine.PutAsync(key, Encoding.UTF8.GetBytes("second"));

            var val = Encoding.UTF8.GetString(_engine.Read(key)!);

            Assert.Equal("second", val);
        }

        [Fact]
        public async Task Recover_From_Disk_Works()
        {
            var key = "persistent";
            var value = Encoding.UTF8.GetBytes("stored-value");
            await _engine.PutAsync(key, value);

            _engine.Dispose();

            // reopen same directory
            using var reopened = new StorageEngine(_tempDir, synchronousWrites: true, writeShardCount: 2);
            var result = reopened.Read(key);

            Assert.NotNull(result);
            Assert.Equal("stored-value", Encoding.UTF8.GetString(result));
        }

        [Fact]
        public async Task Put_LargeValue_Throws()
        {
            var bigValue = new byte[300 * 1024 * 1024]; // > max 256MB

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await _engine.PutAsync("too-big", bigValue));
        }

        [Fact]
        public async Task BatchPut_WithMismatchedKeysAndValues_Throws()
        {
            var keys = new[] { "a", "b" };
            var values = new List<byte[]> { Encoding.UTF8.GetBytes("1") };

            await Assert.ThrowsAsync<ArgumentException>(() =>
                _engine.BatchPutAsync(keys, values));
        }
        
        [Fact]
        public async Task Stress_Put_And_Read_ManyKeys()
        {
            int count = 20_000;
            var keys = Enumerable.Range(0, count).Select(i => $"k{i}").ToArray();
            var values = Enumerable.Range(0, count).Select(i => Encoding.UTF8.GetBytes($"val-{i}")).ToArray();

            var sw = Stopwatch.StartNew();

            // parallel puts
            await Task.WhenAll(keys.Select((k, i) => _engine.PutAsync(k, values[i])));

            sw.Stop();
            var throughput = count / sw.Elapsed.TotalSeconds;
            Assert.True(throughput > 5_000, $"Throughput too low: {throughput} ops/sec");

            // random reads
            var rnd = new Random();
            for (int i = 0; i < 1000; i++)
            {
                var idx = rnd.Next(count);
                var v = _engine.Read(keys[idx]);
                Assert.Equal($"val-{idx}", Encoding.UTF8.GetString(v!));
            }
        }

        [Fact]
        public async Task CrashRecovery_Reopens_And_Retains_Data()
        {
            string key = "crash-key";
            string val = "crash-value";

            await _engine.PutAsync(key, Encoding.UTF8.GetBytes(val));
            _engine.Dispose();

            // Simulate crash: no graceful Dispose, just reopen
            _engine = new StorageEngine(_tempDir, synchronousWrites: true, writeShardCount: 2);

            var result = _engine.Read(key);
            Assert.NotNull(result);
            Assert.Equal(val, Encoding.UTF8.GetString(result));
        }
        
    }
}
