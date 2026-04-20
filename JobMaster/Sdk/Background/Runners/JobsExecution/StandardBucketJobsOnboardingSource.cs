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

    public async Task<bool> PushAsync(JobRawModel job)
    {
        await dispatcher.AddForProcessingAsync(job);
        return true;
    }

    public Task<IList<JobRawModel>> TakeAsync(int count, DateTime scheduledAt)
        => dispatcher.DispatchForProcessingAsync(agentConnectionId, bucketId, count, scheduledAt);
}