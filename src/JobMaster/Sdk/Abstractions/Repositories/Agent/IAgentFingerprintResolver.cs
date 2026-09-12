using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

internal interface IAgentFingerprintResolver
{
    public ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId);
    
    public void Initialize(JobMasterAgentConnectionConfig agentConnConfig);
    
    public string AgentRepoTypeId { get; }
}