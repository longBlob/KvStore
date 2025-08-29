namespace KvStore;

/// <summary>
/// Command-line options for configuring network service.
/// </summary>
public class CliOptions
{
    public string DataDir { get; init; } = "./data/";
    public string ListenPrefix { get; init; } = "http://localhost:8080/";
    public bool IsReplica { get; init; }
    public string[] ReplicaTargets { get; init; } = Array.Empty<string>();
}