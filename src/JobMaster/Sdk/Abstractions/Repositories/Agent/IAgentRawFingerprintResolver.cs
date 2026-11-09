using JobMaster.Sdk.Abstractions.Config;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

internal interface IAgentRawFingerprintResolver
{
    public ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId);

    /// <remarks>Must be called exactly once; a second call throws.</remarks>
    public void Initialize(string agentConnectionId, JobMasterAgentProviderConfig providerConfig);

    public bool IsInitialized { get; }

    public string AgentRepoTypeId { get; }
}
