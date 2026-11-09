namespace JobMaster.Sdk.Ioc.Setup.Json;

internal sealed class AgentConnectionTargetJsonConfig
{
    public string? RepositoryType { get; set; }
    public string? ConnectionString { get; set; }
    public Dictionary<string, object>? ConnectionOptions { get; set; }
}