using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Background.Runners;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Services.Agent;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Sdk.Background.Runners.DrainRunners;

public abstract class DrainJobsRunnerBase : BucketAwareRunner, IDrainJobsRunner
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