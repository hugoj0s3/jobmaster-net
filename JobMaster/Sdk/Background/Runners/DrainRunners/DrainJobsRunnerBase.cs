using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

internal abstract class DrainJobsRunnerBase : BucketAwareRunner, IDrainJobsRunner
{
    protected IMasterBucketsService masterBucketsService;
    protected IAgentJobsDispatcherService agentJobsDispatcherService;
    protected DrainJobsRunnerBase(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
    }

    public void DefineBucketId(string bucketId)
    {
        BucketId = bucketId;
    }
    
    protected async Task HeldJobOnMaster(JobRawModel job)
    {
        job.MarkAsHeldOnMaster();
        await BackgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
    }
}