using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

internal interface IAgentFingerprintResolver
{
    public ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId);

    public void Initialize(JobMasterAgentConnectionConfig agentConnConfig);

    public string AgentRepoTypeId { get; }
}
