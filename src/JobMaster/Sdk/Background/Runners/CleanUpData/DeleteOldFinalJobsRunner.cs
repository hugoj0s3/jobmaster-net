using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Background.Runners.CleanUpData;

/// <summary>
/// Purges finalized jobs (Succeeded, Failed, Cancelled, Aborted) from the master repository
/// once they are older than the cluster's <c>DataRetentionTtl</c>.
/// Skipped when no TTL is configured. A distributed lock prevents concurrent purges across
/// coordinator workers. A <see cref="ConsecutiveBurstLimiter"/> shortens the next interval
/// when a full batch was deleted, allowing the runner to drain large backlogs quickly before
/// returning to its normal <see cref="SucceedInterval"/>.
/// </summary>
internal sealed class DeleteOldFinalJobsRunner : JobMasterRunner
{
    private readonly IMasterClusterConfigurationService clusterConfigService;
    private readonly IMasterJobsRepository jobsRepo;
    private readonly IMasterLogsRepository logsRepo;
    private readonly IMasterDistributedLockerService locker;
    private readonly JobMasterLockKeys lockKeys;
    private readonly ConsecutiveBurstLimiter burstLimiter;

    public DeleteOldFinalJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        clusterConfigService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        jobsRepo = backgroundAgentWorker.GetClusterAwareRepository<IMasterJobsRepository>();
        logsRepo = backgroundAgentWorker.GetClusterAwareRepository<IMasterLogsRepository>();
        locker = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
        burstLimiter = new ConsecutiveBurstLimiter(10, BackgroundAgentWorker.TransferBatchSize);
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
            return OnTickResult.Skipped(this);

        var cfg = clusterConfigService.Get();
        if (cfg == null || cfg.DataRetentionTtl <= TimeSpan.Zero)
            return OnTickResult.Skipped(SucceedInterval);

        var cutoff = DateTime.UtcNow - cfg.DataRetentionTtl;

        var desiredNext = SucceedInterval;
        var burstNext = TimeSpan.FromMinutes(1);
        var lockDuration = burstNext + TimeSpan.FromMinutes(1);

        var lockToken = locker.TryLock(lockKeys.JobsCleanupLock(), lockDuration);
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromMinutes(2));
        }

        try
        {
            var hasArchiveTarget = !string.IsNullOrEmpty(cfg.TargetArchivedClusterId);
            var archiveFactory = hasArchiveTarget
                ? JobMasterClusterAwareComponentFactories.TryGetFactory(cfg.TargetArchivedClusterId!)
                : null;

            if (hasArchiveTarget && archiveFactory == null)
            {
                logger.Critical(
                    $"Archive cluster '{cfg.TargetArchivedClusterId}' is not reachable from this process. " +
                    "Finalized jobs are being purged WITHOUT archiving until this is resolved — jobs are being lost.",
                    JobMasterLogCategory.Cluster,
                    BackgroundAgentWorker.ClusterConnConfig.ClusterId);
            }

            var purgeCount = 0;
            if (archiveFactory != null)
            {
                var archiveService = archiveFactory.GetComponent<IMasterJobIntakeService>();
                var candidates = await jobsRepo.QueryFinalizedToPurgeAsync(cutoff, BackgroundAgentWorker.TransferBatchSize);
                var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(cfg.TargetArchivedClusterId!);
                foreach (var partition in candidates.Partition(maxBatchSize))
                {
                    purgeCount += await ArchivePartitionAsync(archiveService, partition.ToList(), cfg.TargetArchivedClusterId!);
                }
            }
            else
            {
                var purgedIds = await jobsRepo.PurgeFinalizedAsync(cutoff, BackgroundAgentWorker.TransferBatchSize);

                // JobExecution-category logs of a purged job are removed here (archive path: in
                // ArchivePartitionAsync) — DeleteOldLogsRunner deliberately excludes this category from its own
                // blanket purge, so this is the only place these logs are ever removed. Without an archive
                // target they're simply lost, same as the job/execution rows already are in that case.
                var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(BackgroundAgentWorker.ClusterConnConfig.ClusterId);
                foreach (var partition in purgedIds.Partition(maxBatchSize))
                {
                    var idStrings = partition.Select(id => id.ToString("N")).ToList();
                    var logsToDelete = await logsRepo.QueryForReferenceIdsAsync(JobMasterLogCategory.JobExecution, idStrings);
                    if (logsToDelete.Count > 0)
                    {
                        await logsRepo.DeleteByIdsAsync(logsToDelete.Select(l => l.Id).ToList());
                    }
                }
                
                purgeCount = purgedIds.Count;
            }

            var next = burstLimiter.Next(desiredNext, burstNext, purgeCount);
            return OnTickResult.Success(next);
        }
        finally
        {
            locker.ReleaseLock(lockKeys.JobsCleanupLock(), lockToken);
        }
    }

    private async Task<int> ArchivePartitionAsync(IMasterJobIntakeService archiveService, IList<JobRawModel> candidates, string archiveClusterId)
    {
        var candidateIds = candidates.Select(j => j.Id).ToList();
        var executions = await jobsRepo.QueryJobExecutionsForJobsAsync(candidateIds);
        // "N" format (no dashes) -- matches how JobMasterLoggerExtensions stores ReferenceId
        // for every JobExecution-category log (referenceId.ToString("N")).
        var jobLogs = await logsRepo.QueryForReferenceIdsAsync(
            JobMasterLogCategory.JobExecution, candidateIds.Select(id => id.ToString("N")).ToList());

        // Only delete what the archive confirmed; the rest stays here and is retried next tick.
        var confirmedIds = await archiveService.BulkInsertIfNotExistsAsync(candidates, executions, jobLogs);
        var confirmedSet = new HashSet<Guid>(confirmedIds);
        var notConfirmed = candidateIds.Count(id => !confirmedSet.Contains(id));
        if (notConfirmed > 0)
        {
            logger.Error(
                $"{notConfirmed} finalized job(s) could not be confirmed on archive cluster '{archiveClusterId}'. " +
                "They were kept here and will be retried.",
                JobMasterLogCategory.Cluster,
                BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        }

        if (confirmedIds.Count > 0)
        {
            await jobsRepo.DeleteByIdsAsync(confirmedIds);
        }

        // Logs already loaded above: delete those of the confirmed jobs (same as MigrateJobsRunner).
        var confirmedIdStrings = new HashSet<string>(confirmedIds.Select(id => id.ToString("N")));
        var confirmedJobLogs = jobLogs.Where(l => l.ReferenceId != null && confirmedIdStrings.Contains(l.ReferenceId)).ToList();
        if (confirmedJobLogs.Count > 0)
        {
            await logsRepo.DeleteByIdsAsync(confirmedJobLogs.Select(l => l.Id).ToList());
        }

        return confirmedIds.Count;
    }

    public override TimeSpan SucceedInterval
    {
        get
        {
            var ttl = clusterConfigService.Get()?.DataRetentionTtl ?? TimeSpan.Zero;
            if (ttl <= TimeSpan.Zero) return TimeSpan.FromHours(1);
            var ticks = Math.Max(TimeSpan.FromMinutes(5).Ticks, Math.Min(ttl.Ticks / 2, TimeSpan.FromHours(1).Ticks));
            return TimeSpan.FromTicks(ticks);
        }
    }
}
