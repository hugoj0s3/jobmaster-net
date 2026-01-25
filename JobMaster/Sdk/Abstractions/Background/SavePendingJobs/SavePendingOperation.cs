using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Abstractions.Background.SavePendingJobs;

internal class SavePendingOperation
{
    
    protected readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    protected readonly IMasterJobsService masterJobsService;
    protected readonly IMasterBucketsService masterBucketsService;
    protected readonly IWorkerClusterOperations workerClusterOperations;
    protected readonly IJobMasterLogger logger;
    protected readonly IJobMasterBackgroundAgentWorker backgroundAgentWorker;
    private readonly string bucketId;

    public SavePendingOperation(IJobMasterBackgroundAgentWorker backgroundAgentWorker, string bucketId)
    {
        
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        workerClusterOperations = backgroundAgentWorker.GetClusterAwareService<IWorkerClusterOperations>();
        logger = backgroundAgentWorker.GetClusterAwareService<IJobMasterLogger>();

        this.backgroundAgentWorker = backgroundAgentWorker;
        this.bucketId = bucketId;
    }
    
    public async Task<SaveDrainResultCode> SaveDrainSavePendingWithSafeGuardAsync(JobRawModel job)
    {
        try
        { 
            job.MarkAsHeldOnMaster();
            await backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
            return SaveDrainResultCode.Success;
        }
        catch
        {
            try
            {
                await agentJobsDispatcherService.AddSavePendingJobAsync(job);
            }
            catch (Exception e)
            {
                logger.Critical("Failed to add job to queue", JobMasterLogSubjectType.Job, job.Id, exception: e);
                return SaveDrainResultCode.Failed;
            }
        }

        return SaveDrainResultCode.Failed;
    }
    
    public async Task<SaveDrainResultCode> SaveDrainSavePendingAsync(JobRawModel job)
    {
        try
        { 
            job.MarkAsHeldOnMaster();
            await backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
            return SaveDrainResultCode.Success;
        }
        catch
        {
            logger.Error("Failed to hold job on master", JobMasterLogSubjectType.Job, job.Id);
            return SaveDrainResultCode.Failed;
        }
    }
    
    public async Task<SaveDrainResultCode> SaveDrainProcessingAsync(JobRawModel job)
    {
        if (job.ExceedProcessDeadline() && !job.CanHeldOnMasterExceedDeadline())
        {
            return SaveDrainResultCode.Skipped;
        }
        
        try
        { 
            job.MarkAsHeldOnMaster();
            await backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.Upsert(job));
            return SaveDrainResultCode.Success;
        }
        catch
        {
           logger.Error("Failed to hold job on master", JobMasterLogSubjectType.Job, job.Id);
           return SaveDrainResultCode.Failed;
        }
    }
    
    public async Task<AddSavePendingResult> AddSavePendingJobAsync(JobRawModel jobRaw, DateTime cutOffDate)
    {
        // Insert-first flow to avoid extra read; duplicate key maps to AlreadyExists

        if (jobRaw.ScheduledAt > cutOffDate)
        {
            jobRaw.MarkAsHeldOnMaster();
            try
            {
                await workerClusterOperations.ExecWithRetryAsync(o => o.AddAsync(jobRaw), millisecondsToDelay: 25);
            }
            catch (JobDuplicationException ex)
            {
                logger.Debug("Job duplication detected", JobMasterLogSubjectType.Job, jobRaw.Id, exception: ex);
                return new AddSavePendingResult(AddSavePendingResultCode.AlreadyExists);
            }
            catch (Exception ex)
            {
                logger.Error("Failed to add job to processing", JobMasterLogSubjectType.Job, jobRaw.Id, exception: ex);
                throw;
            }
            
            return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMaster);
        }

        var currentBucket = masterBucketsService.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        var engine = backgroundAgentWorker.GetEngine(bucketId);
        // Short-circuit: Try to inject directly into JobsExecutionEngine if on same worker
        if (engine is not null && 
            jobRaw.Status == JobMasterJobStatus.SavePending && 
            currentBucket?.Status == BucketStatus.Active &&
            jobRaw.IsOnBoarding() && 
            engine.OnBoardingControl.CountAvailability() > 0)
        {
            jobRaw.AssignToBucket(backgroundAgentWorker.AgentConnectionId, backgroundAgentWorker.AgentWorkerId, bucketId);
            
            logger.Debug("Short-circuit adding direct into the execution engine", JobMasterLogSubjectType.Job, jobRaw.Id);
            try
            {
                await workerClusterOperations.ExecWithRetryAsync(o => o.AddAsync(jobRaw), millisecondsToDelay: 25);
            }
            catch (JobDuplicationException e)
            {
                logger.Error("Job duplication detected", JobMasterLogSubjectType.Job, jobRaw.Id, exception: e);
                return new AddSavePendingResult(AddSavePendingResultCode.AlreadyExists);
            }
            
            try
            {
                // It better force for short-circuit and it check earlier.
                var result = await engine.TryOnBoardingJobAsync(jobRaw, forceIfNoCapacity: true);
                if (result == OnBoardingResult.Accepted)
                {
                    logger.Debug("Short-circuit accepted", JobMasterLogSubjectType.Job, jobRaw.Id);
                    return new AddSavePendingResult(AddSavePendingResultCode.Published, bucketId, null);
                }
            
                if (result == OnBoardingResult.MovedToMaster)
                {
                    return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMaster, bucketId, null);
                }
            }
            catch (Exception e)
            { 
                logger.Error("Short-circuit failed to add job to processing", JobMasterLogSubjectType.Job, jobRaw.Id, exception: e);
                jobRaw.MarkAsHeldOnMaster();
                await workerClusterOperations.ExecWithRetryAsync(o => o.UpsertAsync(jobRaw), millisecondsToDelay: 25);
                return new AddSavePendingResult(AddSavePendingResultCode.PublishFailed, bucketId: bucketId, exception: e);
            }
        }

        var selectedBucket = await masterBucketsService.SelectBucketAsync(
            JobMasterConstants.BucketFastAllowDiscrepancy,
            jobRaw.Priority,
            jobRaw.WorkerLane);

        if (selectedBucket == null)
        {
            jobRaw.MarkAsHeldOnMaster();
            try
            {
                await workerClusterOperations.ExecWithRetryAsync(o => o.AddAsync(jobRaw), millisecondsToDelay: 25);
            }
            catch (JobDuplicationException)
            {
                logger.Error("Job duplication detected", JobMasterLogSubjectType.Job, jobRaw.Id);
                return new AddSavePendingResult(AddSavePendingResultCode.AlreadyExists);
            }
            
            return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMasterNoBucket);
        }

        var agentWorkerId = selectedBucket.AgentWorkerId!;
        jobRaw.AssignToBucket(selectedBucket.AgentConnectionId, agentWorkerId, selectedBucket.Id);
        try
        {
            await workerClusterOperations.ExecWithRetryAsync(o => o.AddAsync(jobRaw), millisecondsToDelay: 25);
        }
        catch (JobDuplicationException e)
        {
            logger.Error("Job duplication detected", JobMasterLogSubjectType.Job, jobRaw.Id, exception: e);
            return new AddSavePendingResult(AddSavePendingResultCode.AlreadyExists);
        }
        
        try
        {
            logger.Debug($"Publishing job {jobRaw.Id} to agent {agentWorkerId} bucket {selectedBucket.Id}", JobMasterLogSubjectType.Job, jobRaw.Id);
            
            var publishedMessageId = await agentJobsDispatcherService.AddToProcessingAsync(agentWorkerId, selectedBucket.AgentConnectionId, selectedBucket.Id, jobRaw);
            
            return new AddSavePendingResult(AddSavePendingResultCode.Published, bucketId: selectedBucket.Id, publishedMessageId: publishedMessageId);
        }
        catch (PublishOutcomeUnknownException ex)
        { 
            logger.Error("Publish outcome unknown for job; will hold on master", JobMasterLogSubjectType.Job, jobRaw.Id, exception: ex);
            jobRaw.MarkAsHeldOnMaster();
            await workerClusterOperations.ExecWithRetryAsync(o => o.UpsertAsync(jobRaw), millisecondsToDelay: 25);
            return new AddSavePendingResult(AddSavePendingResultCode.HeldOnMasterPublishedUnknown, bucketId: selectedBucket.Id, publishedMessageId: ex.SupposedPublishedId, exception: ex);
        }
        catch (Exception e)
        { 
            logger.Error("Failed to add job to processing", JobMasterLogSubjectType.Job, jobRaw.Id, exception: e);
            jobRaw.MarkAsHeldOnMaster();
            await workerClusterOperations.ExecWithRetryAsync(o => o.UpsertAsync(jobRaw), millisecondsToDelay: 25);
            return new AddSavePendingResult(AddSavePendingResultCode.PublishFailed, bucketId: selectedBucket.Id, exception: e);
        }
    }
}