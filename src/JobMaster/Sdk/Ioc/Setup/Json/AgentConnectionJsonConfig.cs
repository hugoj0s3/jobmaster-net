namespace JobMaster.Sdk.Ioc.Setup.Json;

internal sealed class AgentConnectionJsonConfig
{
    public string? Name { get; set; }
    public string? RepositoryType { get; set; }
    public string? ConnectionString { get; set; }
    public bool? ProtectConnectionChanges { get; set; }
    public Dictionary<string, object>? ConnectionOptions { get; set; }
}
