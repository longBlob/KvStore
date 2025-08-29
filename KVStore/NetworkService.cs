using System.Text.Json;

namespace KvStore;

using System.Net;
using System.Text;

public class NetworkService
{
    private readonly HttpListener _listener = new();
    private readonly StorageEngine _storage;
    private readonly Replicator? _replicator;

    public NetworkService(CliOptions opts)
    {
        _storage = new StorageEngine(opts.DataDir);
        _listener.Prefixes.Add(opts.ListenPrefix);

        if (opts is { IsReplica: true, ReplicaTargets.Length: > 0 })
        {
            _replicator = new Replicator(opts.ReplicaTargets);
            _storage.SetReplicationCallback(record => _replicator!.ReplicateAsync(record));
        }
    }

    public async Task StartAsync()
    {
        _listener.Start();
        while (_listener.IsListening)
        {
            try
            {
                var ctx = await _listener.GetContextAsync();
                _ = Task.Run(() => ProcessRequestAsync(ctx));
            }
            catch (HttpListenerException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break; 
            }
        }
    }

    public async Task StopAsync()
    {
        _listener.Stop();
        _storage.Dispose();
        await Task.CompletedTask;
    }

 private async Task ProcessRequestAsync(HttpListenerContext ctx)
{
    var req = ctx.Request;
    var res = ctx.Response;

    try
    {
        switch (req.HttpMethod)
        {
            case "PUT":
                {
                    string key = req.QueryString["key"] ?? "";
                    string value = req.QueryString["value"] ?? "";

                    if (!string.IsNullOrEmpty(key))
                    {
                        await _storage.PutAsync(key, Encoding.UTF8.GetBytes(value));
                        res.StatusCode = 200;
                    }
                    else
                    {
                        res.StatusCode = 400;
                    }
                    break;
                }

            case "GET":
                {
                    if (req.Url.AbsolutePath.Equals("/range", StringComparison.OrdinalIgnoreCase))
                    {
                        // GET /range?start=a&end=z
                        string start = req.QueryString["start"] ?? "";
                        string end = req.QueryString["end"] ?? "";

                        // Decode bytes to UTF-8 strings before serializing
                        var values = _storage.ReadKeyRange(start, end)
                            .Where(v => v != null)
                            .Select(v => Encoding.UTF8.GetString(v!))
                            .ToArray();

                        var json = JsonSerializer.Serialize(values);
                        var buffer = Encoding.UTF8.GetBytes(json);

                        res.ContentType = "application/json";
                        await res.OutputStream.WriteAsync(buffer, 0, buffer.Length);
                        res.StatusCode = 200;
                    }
                    else
                    {
                        // GET /?key=foo
                        string key = req.QueryString["key"] ?? "";
                        if (!string.IsNullOrEmpty(key))
                        {
                            var bytes = _storage.Read(key);
                            if (bytes != null)
                            {
                                res.ContentType = "application/octet-stream";
                                await res.OutputStream.WriteAsync(bytes, 0, bytes.Length);
                                res.StatusCode = 200;
                            }
                            else res.StatusCode = 404;
                        }
                        else res.StatusCode = 400;
                    }
                    break;
                }

            case "POST":
                {
                    if (req.Url.AbsolutePath.Equals("/batchPut", StringComparison.OrdinalIgnoreCase))
                    {
                        // Body: { "keys": ["k1","k2"], "values": ["v1","v2"] }
                        using var reader = new StreamReader(req.InputStream, Encoding.UTF8);
                        var body = await reader.ReadToEndAsync();

                        var payload = JsonSerializer.Deserialize<BatchPutRequest>(body);
                        if (payload == null || payload.Keys.Count != payload.Values.Count)
                        {
                            res.StatusCode = 400;
                            break;
                        }

                        var valueBytes = payload.Values.Select(v => Encoding.UTF8.GetBytes(v)).ToList();
                        await _storage.BatchPutAsync(payload.Keys, valueBytes);

                        res.StatusCode = 200;
                    }
                    else
                    {
                        res.StatusCode = 404;
                    }
                    break;
                }

            case "DELETE":
                {
                    string key = req.QueryString["key"] ?? "";
                    if (!string.IsNullOrEmpty(key))
                    {
                        await _storage.DeleteAsync(key);
                        res.StatusCode = 200;
                    }
                    else res.StatusCode = 400;
                    break;
                }

            default:
                res.StatusCode = 405;
                break;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex);
        res.StatusCode = 500;
    }
    finally
    {
        res.OutputStream.Close();
    }
}

private record BatchPutRequest(List<string> Keys, List<string> Values);

}
