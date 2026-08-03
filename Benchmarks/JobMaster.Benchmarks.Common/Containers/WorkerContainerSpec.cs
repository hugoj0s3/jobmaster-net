namespace JobMaster.Benchmarks.Common.Containers;

public sealed class WorkerContainerSpec
{
    public required string Name { get; init; }
    public required IReadOnlyDictionary<string, string> EnvironmentVariables { get; init; }
    public required string DockerfilePath { get; init; }
    public int HttpPort { get; init; } = 8080;
    public string HealthCheckPath { get; init; } = "/health";
}
