namespace KvStore;

using System.Text;
using System.Text.Json;

public class Replicator
{
    private readonly string[] _targets;
    private readonly HttpClient _http = new();

    public Replicator(string[] targets) => _targets = targets;

    public async Task ReplicateAsync(ReplicationRecord record)
    {
        var json = JsonSerializer.Serialize(record);

        var tasks = _targets.Select(async target =>
        {
            try
            {
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                await _http.PostAsync(target, content);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        });


        await Task.WhenAll(tasks);
    }
}

public record ReplicationRecord(string Key, byte[] Value, bool Deleted)
{
    public static ReplicationRecord Put(string key, byte[] value) => new(key, value, false);
    public static ReplicationRecord Del(string key) => new(key, Array.Empty<byte>(), true);
}