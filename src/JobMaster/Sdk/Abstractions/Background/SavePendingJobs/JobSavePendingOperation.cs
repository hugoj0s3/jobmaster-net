using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Abstractions.Background.SavePendingJobs;

/// <summary>
/// Saves <see cref="JobRawModel"/>s that arrive in the <c>PendingSave</c> state on master, for normal ingestion and
/// bucket drains. A job is held on master or assigned to a bucket (<see cref="AssignAsync"/>); once inserted, an
/// assigned job is dispatched through <see cref="IWorkerClusterOperations.BulkDispatchJobsToBucketAsync"/> (this
/// process's engine for that bucket, or a publish). Polling runners save leased batches
/// (<see cref="SaveAndAckAsync"/>), the NATS runner one message at a time (<see cref="SaveAndDispatchAsync"/>).
/// </summary>
internal class JobSavePendingOperation
{
    
    protected readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    protected readonly IMasterJobsService masterJobsService;
    protected readonly IMasterBucketsService masterBucketsService;
    protected readonly IWorkerClusterOperations workerClusterOperations;
    protected readonly IJobMasterLogger logger;
    protected readonly IJobMasterBackgroundAgentWorker backgroundAgentWorker;
    private readonly string bucketId;

    /// <summary>
    /// How long a save-pending batch is leased (jobs and recurring schedules): room for the whole batch, 0.5 s per
    /// message, at least 1.5 min, at most 10 min. A lease that runs out only costs duplicate work; a long one keeps a
    /// stopped worker's messages hidden longer.
    /// </summary>
    internal static TimeSpan LeaseFor(int bufferSize) =>
        JobMasterMathUtil.Clamp(TimeSpan.FromSeconds(bufferSize * 0.5), TimeSpan.FromSeconds(90), TimeSpan.FromMinutes(10));

    /// <summary>
    /// Initializes the operation by resolving all required cluster-aware services from the
    /// supplied <paramref name="backgroundAgentWorker"/> and binding it to the given
    /// <paramref name="bucketId"/>.
    /// </summary>
    /// <param name="backgroundAgentWorker">
    /// The background agent worker that provides cluster-scoped service resolution and engine access.
    /// </param>
    /// <param name="bucketId">
    /// The bucket this operation is associated with, used for engine look-ups and fallback bucket retrieval.
    /// </param>
    public JobSavePendingOperation(IJobMasterBackgroundAgentWorker backgroundAgentWorker, string bucketId)
    {
        
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        workerClusterOperations = backgroundAgentWorker.GetClusterAwareService<IWorkerClusterOperations>();
        logger = backgroundAgentWorker.GetClusterAwareService<IJobMasterLogger>();

        this.backgroundAgentWorker = backgroundAgentWorker;
        this.bucketId = bucketId;
    }
    
    
    /// <summary>
    /// Holds the jobs of a draining bucket's processing queue on master. A processing job is always on master already,
    /// so this only updates it, and only where master's version still matches the message's copy: a job that changed
    /// since (taken back by its deadline and given to another bucket, finished, deleted) is left as is. Partitioned by
    /// the master batch size; a failed partition is logged and the others still run. Returns false when a partition
    /// failed; the job is still on master either way, and the deadline check brings back one left in this bucket.
    /// </summary>
    public async Task<bool> HoldDrainedJobsOnMasterAsync(IList<JobRawModel> jobs)
    {
        var allSucceed = true;
        var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        foreach (var partition in jobs.Partition(maxBatchSize))
        {
            try
            {
                var partitionJobs = partition.ToList();
                foreach (var job in partitionJobs)
                {
                    job.MarkAsHeldOnMaster();
                }

                var updated = await backgroundAgentWorker.WorkerClusterOperations.ExecWithRetryAsync(o => o.BulkUpdateAsync(partitionJobs));
                var updatedIds = new HashSet<Guid>(updated.Select(x => x.Id));
                foreach (var job in partitionJobs.Where(x => !updatedIds.Contains(x.Id)))
                {
                    logger.Warn($"Drain: job not held on master, it changed on master since it was sent to bucket {bucketId}.", JobMasterLogCategory.Job, job.Id);
                }
            }
            catch (Exception e)
            {
                logger.Error($"Drain: failed to hold a partition of {partition.Count} job(s) on master for bucket {bucketId}. Continuing with the remaining partitions.",
                    JobMasterLogCategory.Bucket, bucketId, exception: e);
                allSucceed = false;
            }
        }

        return allSucceed;
    }

    /// <summary>
    /// Decides where a pending-save job goes and applies it to the job: held on master (past
    /// <paramref name="cutOffDate"/>, or no bucket available; returns null) or assigned to a bucket (returned).
    /// A job in the onboarding window goes to <paramref name="currentRunnerBucket"/> while its engine on this worker
    /// has room: it then starts from memory, with no processing message to write and pull. Otherwise the bucket
    /// <c>SelectBucketAsync</c> picks. Which engine or queue then runs it is the dispatch's choice.
    /// </summary>
    public async Task<BucketModel?> AssignAsync(BucketModel? currentRunnerBucket, JobRawModel job, DateTime cutOffDate)
    {
        if (job.GetSafeNextPlanExecutionAt() > cutOffDate)
        {
            job.MarkAsHeldOnMaster();
            return null;
        }
        
        var engine = 
            currentRunnerBucket is null ? null :
            backgroundAgentWorker.GetEngine(currentRunnerBucket!.Id);
        if (engine is not null && job.IsWithinOnboardingWindow() && engine.HasOnBoardingAvailability())
        {
            job.AssignToBucket(currentRunnerBucket!);
            return currentRunnerBucket!;
        }

        var selectedBucket = await masterBucketsService.SelectBucketAsync(
            JobMasterConstants.BucketFastAllowDiscrepancy,
            job.Priority,
            job.WorkerLane);

        if (selectedBucket is null)
        {
            job.MarkAsHeldOnMaster();
            return null;
        }

        job.AssignToBucket(selectedBucket);
        return selectedBucket;
    }

    /// <summary>
    /// Saves one pending-save job (already assigned by <see cref="AssignAsync"/>) on master and, if this call inserted
    /// it into a bucket, dispatches it. Returns whether the job is on master now (inserted or already there); false
    /// means not saved, so the caller keeps its message.
    /// </summary>
    public async Task<bool> SaveAndDispatchAsync(JobRawModel job, BucketModel? targetBucket)
    {
        var result = await masterJobsService.BulkInsertIfNotExistsAsync(new List<JobRawModel> { job });
        if (result[BulkInsertStatus.Unknow].Contains(job.Id))
        {
            return false;
        }

        // An AlreadyInserted job was saved, and dispatched, before.
        if (targetBucket is not null && result[BulkInsertStatus.Inserted].Contains(job.Id))
        {
            try
            {
                await workerClusterOperations
                    .BulkDispatchJobsToBucketAsync(backgroundAgentWorker, targetBucket, new List<JobRawModel> { job });
            }
            catch (Exception e)
            {
                // Only log here: the dispatch holds a failed job back on master itself.
                logger.Error($"Failed to dispatch job to bucket {targetBucket.Id}.", JobMasterLogCategory.Job, job.Id, exception: e);
            }
        }

        return true;
    }

    /// <summary>
    /// Saves leased pending-save jobs on master, acks the messages of every job now on master, then dispatches the
    /// ones this call inserted to the bucket they were assigned to (<paramref name="bucketAssignments"/>; a job not in
    /// it is held on master). A message not acked (job not confirmed on master) is leased again later.
    /// Returns false when a partition failed or a job wasn't confirmed on master (the others are still saved), so the
    /// runner counts the tick as failed.
    /// </summary>
    public async Task<bool> SaveAndAckAsync(
        BucketModel currentRunnerBucket,
        IList<(JobRawModel Job, string AckKey)> leasedJobs,
        IDictionary<Guid, BucketModel> bucketAssignments)
    {
        var allSaved = true;
        // The same job can arrive in more than one message: save it once, ack all of them.
        var ackKeysByJobId = leasedJobs.ToLookup(x => x.Job.Id, x => x.AckKey);
        var jobs = leasedJobs.Select(x => x.Job).DistinctBy(x => x.Id).ToList();

        var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        foreach (var partition in jobs.Partition(maxBatchSize))
        {
            try
            {
                if (!await SaveAndAckPartitionAsync(currentRunnerBucket, partition.ToList(), bucketAssignments, ackKeysByJobId))
                {
                    allSaved = false;
                }
            }
            catch (Exception e)
            {
                // The partition's unacked messages are leased again; continue with the rest of the batch.
                logger.Error($"Failed to save a partition of {partition.Count} pending job(s) for bucket {bucketId}. Continuing with the remaining partitions.",
                    JobMasterLogCategory.Bucket, bucketId, exception: e);
                allSaved = false;
            }
        }

        return allSaved;
    }

    // Returns whether every job of the partition is on master (none Unknow).
    private async Task<bool> SaveAndAckPartitionAsync(
        BucketModel currentRunnerBucket,
        IList<JobRawModel> jobs,
        IDictionary<Guid, BucketModel> bucketAssignments,
        ILookup<Guid, string> ackKeysByJobId)
    {
        var bulkResult = await masterJobsService.BulkInsertIfNotExistsAsync(jobs);

        // On master now (inserted or already there), so master owns the job: its messages can go even if the
        // dispatch below fails (the dispatch holds a failed publish back on master; the bucket's deadline check
        // catches the rest). Unknow ones are leased again.
        var ackKeys = bulkResult[BulkInsertStatus.Inserted]
            .Concat(bulkResult[BulkInsertStatus.AlreadyInserted])
            .SelectMany(id => ackKeysByJobId[id])
            .ToList();
        await agentJobsDispatcherService.AckSavePendingJobsAsync(currentRunnerBucket.AgentConnectionId, bucketId, ackKeys);

        // Only what this call inserted is dispatched: an AlreadyInserted job was saved, and dispatched, before.
        var insertedIds = new HashSet<Guid>(bulkResult[BulkInsertStatus.Inserted]);
        var jobsByBucket = jobs
            .Where(x => insertedIds.Contains(x.Id) && bucketAssignments.ContainsKey(x.Id))
            .GroupBy(x => bucketAssignments[x.Id].Id);

        foreach (var bucketJobs in jobsByBucket)
        {
            var targetBucket = bucketAssignments[bucketJobs.First().Id];
            var agentMaxBatchSize = OperationThrottlerSettingsFactory.GetAgentExecutionMaxBatchSize(targetBucket.AgentConnectionId.IdValue);
            foreach (var partition in bucketJobs.ToList().Partition(agentMaxBatchSize))
            {
                try
                {
                    // Short-circuits into the bucket's engine when it runs in this process, publishes otherwise.
                    await workerClusterOperations.BulkDispatchJobsToBucketAsync(backgroundAgentWorker, targetBucket, partition.ToList());
                }
                catch (Exception e)
                {
                    // Only log here: the dispatch holds a failed partition back on master itself.
                    logger.Error($"Failed to dispatch a partition of {partition.Count} job(s) to bucket {targetBucket.Id}. Continuing with the remaining partitions.",
                        JobMasterLogCategory.Bucket, targetBucket.Id, exception: e);
                }
            }
        }

        return bulkResult[BulkInsertStatus.Unknow].Count == 0;
    }
}