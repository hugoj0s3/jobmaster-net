using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Repositories;

internal sealed class HybridAgentFingerprintResolver : AgentFingerprintResolver
{
    public HybridAgentFingerprintResolver(
        string agentRepoTypeId,
        IAgentRawFingerprintResolver transportResolver,
        IAgentRawFingerprintResolver executionResolver)
        : base(agentRepoTypeId, transportResolver, executionResolver)
    {
    }

    public override async ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId)
    {
        var transportFp = await TransportResolver.GiveYourFingerprintAsync(clusterId, agentConnectionId);
        var executionFp = await ExecutionResolver.GiveYourFingerprintAsync(clusterId, agentConnectionId);
        return RepoTypeAndFingerprintUtil.FormatHybridFingerprint(transportFp, executionFp);
    }
}
