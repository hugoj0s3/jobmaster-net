using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingJobs;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Background.Runners.SavePendingJobs;

/// <summary>
/// Saves the <c>PendingSave</c> jobs queued for a specific bucket on master.
/// Must be activated via <see cref="DefineBucketId"/> before ticking; skips immediately
/// if no bucket ID is set or the bucket is not in an Active or Completing state.
/// Each tick leases a batch, picks each job's destination (held on master past the transient threshold or when no
/// bucket is available, this worker's engine, or a selected bucket), bulk inserts it, acks the messages of every job
/// now on master, then dispatches the jobs this tick inserted. A message not acked (job not confirmed on master, or
/// the worker stopped) is leased again once its lease (<see cref="JobSavePendingOperation.LeaseFor"/>) ends.
/// Runs every <see cref="SucceedInterval"/> while the bucket is healthy.
/// </summary>
internal class PollingSavePendingJobsRunner : BucketAwareRunner, ISavePendingJobsRunner
{
    protected readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    protected readonly IMasterBucketsService masterBucketsService;
    protected readonly IMasterClusterConfigurationService MasterClusterConfigurationService;

    private readonly TimeSpan interval = TimeSpan.FromSeconds(2.5);

    public override TimeSpan SucceedInterval => interval;

    private JobSavePendingOperation? savePendingOperation;

    public PollingSavePendingJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        MasterClusterConfigurationService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
    }
    
    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        
        if (string.IsNullOrEmpty(BucketId))
        {
            return OnTickResult.Skipped(this);
        }

        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || (bucket.Status != BucketStatus.Active && bucket.Status != BucketStatus.Completing))
        {
            return OnTickResult.Skipped(this);
        }

        var leasedJobs = await agentJobsDispatcherService
            .LeaseSavePendingJobsAsync(bucket.AgentConnectionId, BucketId!, BackgroundAgentWorker.BucketBufferSize, JobSavePendingOperation.LeaseFor(BackgroundAgentWorker.BucketBufferSize));
        if (leasedJobs.Count <= 0)
        {
            return OnTickResult.Skipped(TimeSpan.FromMilliseconds(interval.TotalMilliseconds * 5));
        }

        savePendingOperation ??= new JobSavePendingOperation(BackgroundAgentWorker, BucketId!);

        // The same job can arrive in more than one message; SaveAndAckAsync keeps the first one, as here.
        var jobs = leasedJobs.Select(x => x.Job).DistinctBy(x => x.Id).ToList();

        var configuration = MasterClusterConfigurationService.Get();
        var transientThreshold = configuration?.TransientThreshold ?? TimeSpan.FromMinutes(5);

        DateTime cutOffDate = DateTime.UtcNow.Add(transientThreshold);

        // Held on master or assigned to a bucket; the dispatch decides between this process's engine for that bucket
        // and a publish.
        var bucketAssignments = new Dictionary<Guid, BucketModel>();
        foreach (var job in jobs)
        {
            var targetBucket = await savePendingOperation.AssignAsync(bucket, job, cutOffDate);
            if (targetBucket is not null)
            {
                bucketAssignments.Add(job.Id, targetBucket);
            }
        }

        var saved = await savePendingOperation.SaveAndAckAsync(bucket, leasedJobs, bucketAssignments);
        return saved ? OnTickResult.Success(this) : OnTickResult.Failed(this);
     }
    
    public void DefineBucketId(string bucketId)
    {
        if (string.IsNullOrEmpty(bucketId))
            throw new ArgumentNullException(nameof(bucketId));
        
        if (!string.IsNullOrEmpty(BucketId))
            throw new InvalidOperationException("BucketId is already defined.");
        
        this.BucketId = bucketId;
    }
}
