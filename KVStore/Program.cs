namespace KvStore;

public class Program
{
    static async Task Main(string[] args)
    {
        var options = new CliOptions
        {
            DataDir = "./data/",
            ListenPrefix = "http://localhost:8080/",
            IsReplica = false,
            ReplicaTargets = new string[]
            {
                "http://localhost:8081/",
                "http://localhost:8082/"
            }
        };


        var networkService = new NetworkService(options);
        Console.WriteLine($"Starting KV store HTTP server at {options.ListenPrefix}");
        var serverTask = networkService.StartAsync();


        Console.CancelKeyPress += async (sender, eventArgs) =>
        {
            Console.WriteLine("Shutting down KV store...");
            eventArgs.Cancel = true; // prevent immediate termination
            await networkService.StopAsync();
        };


        await serverTask;

        Console.WriteLine("KV store stopped.");
    }
}