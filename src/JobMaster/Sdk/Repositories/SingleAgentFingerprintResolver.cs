using JobMaster.Sdk.Abstractions.Repositories.Agent;

namespace JobMaster.Sdk.Repositories;

internal sealed class SingleAgentFingerprintResolver : AgentFingerprintResolver
{
    public SingleAgentFingerprintResolver(string agentRepoTypeId, IAgentRawFingerprintResolver resolver)
        : base(agentRepoTypeId, resolver, resolver)
    {
    }

    // Returned as-is (never hybrid-formatted) so fingerprints already stored on master keep matching.
    public override ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId)
        => TransportResolver.GiveYourFingerprintAsync(clusterId, agentConnectionId);
}
