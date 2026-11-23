using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Ioc.Definitions;

internal sealed class AgentProviderDefinition
{
    public string? AgentRepoType { get; set; }
    public string? AgentConnString { get; set; }
    public JobMasterConfigDictionary? AgentAdditionalConnConfig { get; set; }
}
