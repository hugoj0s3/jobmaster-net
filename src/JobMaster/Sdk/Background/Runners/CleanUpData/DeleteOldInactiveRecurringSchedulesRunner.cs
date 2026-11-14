using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Background.Runners.CleanUpData;

/// <summary>
/// Purges terminated recurring schedules from the master repository once they are older
/// than the cluster's <c>DataRetentionTtl</c> (only schedules with a non-null
/// <c>TerminatedAt</c> are eligible).
/// Skipped when no TTL is configured. A distributed lock prevents concurrent purges across
/// coordinator workers. A <see cref="ConsecutiveBurstLimiter"/> shortens the next interval
/// when a full batch was deleted, allowing the runner to drain large backlogs quickly before
/// returning to its normal <see cref="SucceedInterval"/>
/// </summary>
internal sealed class DeleteOldInactiveRecurringSchedulesRunner : JobMasterRunner
{
    private readonly IMasterClusterConfigurationService clusterConfigService;
    private readonly IMasterRecurringSchedulesRepository schedulesRepo;
    private readonly IMasterDistributedLockerService locker;
    private readonly JobMasterLockKeys lockKeys;
    private readonly ConsecutiveBurstLimiter burstLimiter;

    public DeleteOldInactiveRecurringSchedulesRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        clusterConfigService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        schedulesRepo = backgroundAgentWorker.GetClusterAwareRepository<IMasterRecurringSchedulesRepository>();
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

        var lockToken = locker.TryLock(lockKeys.RecurringSchedulesCleanupLock(), lockDuration);
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromMinutes(2));
        }

        try
        {
            var deleted = 0;
            var hasArchiveTarget = !string.IsNullOrEmpty(cfg.TargetArchivedClusterId);
            var archiveFactory = hasArchiveTarget
                ? JobMasterClusterAwareComponentFactories.TryGetFactory(cfg.TargetArchivedClusterId!)
                : null;

            if (hasArchiveTarget && archiveFactory == null)
            {
                logger.Critical(
                    $"Archive cluster '{cfg.TargetArchivedClusterId}' is not reachable from this process. " +
                    "Terminated recurring schedules are being purged WITHOUT archiving until this is resolved — data is being lost.",
                    JobMasterLogCategory.Cluster,
                    BackgroundAgentWorker.ClusterConnConfig.ClusterId);
            }

            if (archiveFactory != null)
            {
                var archiveService = archiveFactory.GetComponent<IMasterRecurringScheduleIntakeService>();
                var candidates = await schedulesRepo.QueryTerminatedToPurgeAsync(cutoff, BackgroundAgentWorker.TransferBatchSize);
                var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(cfg.TargetArchivedClusterId!);
                foreach (var partition in candidates.Partition(maxBatchSize))
                {
                    deleted += await ArchivePartitionAsync(archiveService, partition.ToList(), cfg.TargetArchivedClusterId!);
                }
            }
            else
            {
                deleted = await schedulesRepo.PurgeTerminatedAsync(cutoff, BackgroundAgentWorker.TransferBatchSize);
            }

            var next = burstLimiter.Next(desiredNext, burstNext, deleted);
            return OnTickResult.Success(next);
        }
        finally
        {
            locker.ReleaseLock(lockKeys.RecurringSchedulesCleanupLock(), lockToken);
        }
    }

    private async Task<int> ArchivePartitionAsync(IMasterRecurringScheduleIntakeService archiveService, IList<RecurringScheduleRawModel> candidates, string archiveClusterId)
    {
        // Only delete what the archive confirmed; the rest stays here and is retried next tick.
        var confirmedIds = await archiveService.BulkInsertIfNotExistsAsync(candidates);
        var confirmedSet = new HashSet<Guid>(confirmedIds);
        var notConfirmed = candidates.Count(s => !confirmedSet.Contains(s.Id));
        if (notConfirmed > 0)
        {
            logger.Error(
                $"{notConfirmed} terminated recurring schedule(s) could not be confirmed on archive cluster '{archiveClusterId}'. " +
                "They were kept here and will be retried.",
                JobMasterLogCategory.Cluster,
                BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        }

        return confirmedIds.Count > 0 ? await schedulesRepo.DeleteByIdsAsync(confirmedIds) : 0;
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
