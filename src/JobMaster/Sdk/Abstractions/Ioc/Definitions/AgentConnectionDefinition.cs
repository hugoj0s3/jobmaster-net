using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Ioc.Definitions;

public sealed class AgentConnectionDefinition
{
    public string ClusterId { get; set; } = string.Empty;
    public string AgentConnectionName { get; set; } = string.Empty;
    public string? AgentRepoType { get; set; }
    public string? AgentConnString { get; set; }
    public int? RuntimeDbOperationThrottleLimit { get; set; }
    public JobMasterConfigDictionary? AgentAdditionalConnConfig { get; set; }
}