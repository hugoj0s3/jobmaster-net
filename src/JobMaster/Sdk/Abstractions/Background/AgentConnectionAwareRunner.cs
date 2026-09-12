using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Background;

/// <summary>
/// Base for runners that need the worker's own <see cref="AgentConnectionId"/> but aren't scoped to a
/// single bucket (unlike <see cref="BucketAwareRunner"/>, which resolves its connection from the bucket
/// it's assigned to). Only ever constructed from Full/Execution/Drain-mode loader paths in
/// <c>JobMasterBackgroundAgentWorker</c>, which resolve the connection once (from
/// <c>JobMasterAgentConnectionConfig</c>, guaranteed non-null for every non-Coordinator worker) and pass
/// it down explicitly — there is no <c>AgentConnectionId</c> property on
/// <c>IJobMasterBackgroundAgentWorker</c> at all, so a Coordinator-mode caller can't accidentally reach a
/// runner that needs one without the compiler flagging the missing argument.
/// </summary>
internal abstract class AgentConnectionAwareRunner : JobMasterRunner
{
    public AgentConnectionId AgentConnectionId { get; }

    protected AgentConnectionAwareRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker,
        AgentConnectionId agentConnectionId,
        bool bucketAwareLifeCycle,
        bool useSemaphore = true)
        : base(backgroundAgentWorker, bucketAwareLifeCycle, useSemaphore)
    {
        AgentConnectionId = agentConnectionId;
    }
}
