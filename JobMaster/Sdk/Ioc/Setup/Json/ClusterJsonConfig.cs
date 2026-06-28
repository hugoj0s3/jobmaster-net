namespace JobMaster.Sdk.Ioc.Setup.Json;

internal sealed class ClusterJsonConfig
{
    public string? ClusterId { get; set; }
    public bool SetAsDefault { get; set; }
    public bool Standalone { get; set; }
    public string? Mode { get; set; }
    public string? RepoType { get; set; }
    public string? ConnectionString { get; set; }
    public Dictionary<string, object>? ConnectionOptions { get; set; }
    public string? TransientThreshold { get; set; }
    public string? DefaultJobTimeout { get; set; }
    public int? DefaultMaxRetryCount { get; set; }
    public int? MaxMessageByteSize { get; set; }
    public string? IanaTimeZoneId { get; set; }
    public List<AgentConnectionJsonConfig>? AgentConnections { get; set; }
    public List<WorkerJsonConfig>? Workers { get; set; }
}
