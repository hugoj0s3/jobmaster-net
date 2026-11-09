using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Repositories.Agent;

namespace JobMaster.Sdk.Repositories;

internal abstract class AgentFingerprintResolver : IAgentFingerprintResolver
{
    private readonly object initializeLock = new();
    private bool isInitialized;

    protected AgentFingerprintResolver(
        string agentRepoTypeId,
        IAgentRawFingerprintResolver transportResolver,
        IAgentRawFingerprintResolver executionResolver)
    {
        AgentRepoTypeId = agentRepoTypeId;
        TransportResolver = transportResolver ?? throw new ArgumentNullException(nameof(transportResolver));
        ExecutionResolver = executionResolver ?? throw new ArgumentNullException(nameof(executionResolver));
    }

    public string AgentRepoTypeId { get; }

    protected IAgentRawFingerprintResolver TransportResolver { get; }
    protected IAgentRawFingerprintResolver ExecutionResolver { get; }

    public void Initialize(JobMasterAgentConnectionConfig agentConnConfig)
    {
        lock (initializeLock)
        {
            if (isInitialized)
                throw new InvalidOperationException($"{GetType().Name} is already initialized.");

            // Single connections share one raw resolver for both roles, so the second check skips it.
            if (!TransportResolver.IsInitialized)
                TransportResolver.Initialize(agentConnConfig.Id, agentConnConfig.TransportConfig);
            if (!ExecutionResolver.IsInitialized)
                ExecutionResolver.Initialize(agentConnConfig.Id, agentConnConfig.ExecutionConfig);

            isInitialized = true;
        }
    }

    public abstract ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId);
}
