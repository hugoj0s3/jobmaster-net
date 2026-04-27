using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Services.Agent;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

internal sealed class StandardBucketJobsOnboardingSource : IJobsOnboardingSource
{
    private readonly IAgentJobsDispatcherService dispatcher;
    private readonly AgentConnectionId agentConnectionId;
    private readonly string bucketId;

    public StandardBucketJobsOnboardingSource(
        IAgentJobsDispatcherService dispatcher,
        AgentConnectionId agentConnectionId,
        string bucketId)
    {
        this.dispatcher = dispatcher;
        this.agentConnectionId = agentConnectionId;
        this.bucketId = bucketId;
    }

    public Task<IList<JobRawModel>> PullAsync(int count, DateTime scheduledAt)
        => dispatcher.PullForProcessingAsync(agentConnectionId, bucketId, count, scheduledAt);
}
