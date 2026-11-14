using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Background.Runners.Migration;

/// <summary>
/// Continuously forwards <c>Active</c> recurring schedules on a <c>ClusterMode.Migrating</c> or
/// <c>ClusterMode.Archived</c> cluster to its <c>TargetActiveClusterId</c> cluster (on Archived: schedules
/// that landed after the switch; no target there is logged Critical). Recurring schedules have no "held" sub-status the way jobs
/// do — they're persisted <c>Active</c> immediately even under Migrating mode, but nothing ever plans
/// their next occurrences there (<c>ScheduleRecurringJobsRunner</c> only runs on <c>Active</c> clusters),
/// so every <c>Active</c> schedule on a Migrating cluster is a migration candidate. If the target
/// cluster isn't reachable, nothing is deleted here — schedules simply stay until the next tick.
/// </summary>
internal sealed class MigrateRecurringSchedulesRunner : JobMasterRunner
{
    private readonly IMasterClusterConfigurationService clusterConfigService;
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly IMasterRecurringSchedulesRepository schedulesRepo;

    public MigrateRecurringSchedulesRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        clusterConfigService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        schedulesRepo = backgroundAgentWorker.GetClusterAwareRepository<IMasterRecurringSchedulesRepository>();
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
            return OnTickResult.Skipped(this);

        var cfg = clusterConfigService.Get();
        if (cfg == null)
            return OnTickResult.Skipped(this);

        var startTimeUtc = DateTime.UtcNow;
        var criteria = new RecurringScheduleQueryCriteria
        {
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            Status = RecurringScheduleStatus.Active,
        };

        var count = await masterRecurringSchedulesService.ProbeCountForAcquireAsync(criteria);
        if (count <= 0)
            return OnTickResult.Skipped(this);

        var isArchived = cfg.ClusterMode == ClusterMode.Archived;
        if (string.IsNullOrEmpty(cfg.TargetActiveClusterId))
        {
            if (isArchived)
            {
                logger.Critical(
                    $"Archived cluster '{BackgroundAgentWorker.ClusterConnConfig.ClusterId}' holds {count} active recurring schedule(s) but has no cluster to move them to " +
                    "(it wasn't switched to Archived from Migrating). They will not run.",
                    JobMasterLogCategory.Cluster,
                    BackgroundAgentWorker.ClusterConnConfig.ClusterId);
            }

            return OnTickResult.Skipped(this);
        }

        var targetFactory = JobMasterClusterAwareComponentFactories.TryGetFactory(cfg.TargetActiveClusterId!);
        if (targetFactory == null)
        {
            var message =
                $"Target active cluster '{cfg.TargetActiveClusterId}' is not reachable from this process. " +
                "Recurring schedules remain on this cluster until this is resolved — nothing is lost.";
            if (isArchived)
                logger.Critical(message, JobMasterLogCategory.Cluster, BackgroundAgentWorker.ClusterConnConfig.ClusterId);
            else
                logger.Error(message, JobMasterLogCategory.Cluster, BackgroundAgentWorker.ClusterConnConfig.ClusterId);
            return OnTickResult.Skipped(this);
        }

        var schedules = await masterRecurringSchedulesService.AcquireAndFetchAsync(criteria, startTimeUtc.Add(JobMasterConstants.DurationToLockRecords));
        if (schedules.Count == 0)
            return OnTickResult.Skipped(this);

        var intakeService = targetFactory.GetComponent<IMasterRecurringScheduleIntakeService>();
        var maxBatchSize = OperationThrottlerSettingsFactory.GetMasterMaxBatchSize(cfg.TargetActiveClusterId!);
        foreach (var partition in schedules.Partition(maxBatchSize))
        {
            await MigratePartitionAsync(intakeService, partition.ToList(), cfg.TargetActiveClusterId!);
        }

        return OnTickResult.Success(this);
    }

    private async Task MigratePartitionAsync(IMasterRecurringScheduleIntakeService intakeService, IList<RecurringScheduleRawModel> schedules, string targetClusterId)
    {
        // Only delete what the target confirmed; the rest stays here and is retried once its lock expires.
        var confirmedIds = await intakeService.BulkInsertIfNotExistsAsync(schedules);
        var confirmedSet = new HashSet<Guid>(confirmedIds);
        var notConfirmed = schedules.Count(s => !confirmedSet.Contains(s.Id));
        if (notConfirmed > 0)
        {
            logger.Error(
                $"{notConfirmed} recurring schedule(s) could not be confirmed on target cluster '{targetClusterId}'. " +
                "They were kept here and will be retried.",
                JobMasterLogCategory.Cluster,
                BackgroundAgentWorker.ClusterConnConfig.ClusterId);
        }

        if (confirmedIds.Count > 0)
        {
            await schedulesRepo.DeleteByIdsAsync(confirmedIds);
        }
    }

    // Archived clusters only get the odd late schedule; Migrating ones are draining real traffic.
    public override TimeSpan SucceedInterval =>
        clusterConfigService.Get()?.ClusterMode == ClusterMode.Archived ? TimeSpan.FromMinutes(2.5) : TimeSpan.FromSeconds(30);
}
