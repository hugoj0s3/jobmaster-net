using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.Migration;

/// <summary>
/// Continuously forwards <c>Active</c> recurring schedules on a <c>ClusterMode.Migrating</c> cluster to
/// its <c>TargetActiveClusterId</c> cluster. Recurring schedules have no "held" sub-status the way jobs
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
        if (cfg == null || string.IsNullOrEmpty(cfg.TargetActiveClusterId))
            return OnTickResult.Skipped(this);

        var targetFactory = JobMasterClusterAwareComponentFactories.TryGetFactory(cfg.TargetActiveClusterId!);
        if (targetFactory == null)
        {
            logger.Error(
                $"Target active cluster '{cfg.TargetActiveClusterId}' is not reachable from this process. " +
                "Recurring schedules remain on this cluster until this is resolved — nothing is lost.",
                JobMasterLogCategory.Cluster,
                BackgroundAgentWorker.ClusterConnConfig.ClusterId);
            return OnTickResult.Skipped(this);
        }

        var startTimeUtc = DateTime.UtcNow;
        var criteria = new RecurringScheduleQueryCriteria
        {
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            Status = RecurringScheduleStatus.Active,
        };

        var schedules = await masterRecurringSchedulesService.AcquireAndFetchAsync(criteria, startTimeUtc.Add(JobMasterConstants.DurationToLockRecords));
        if (schedules.Count == 0)
            return OnTickResult.Skipped(this);

        var intakeService = targetFactory.GetComponent<IMasterRecurringScheduleIntakeService>();
        await intakeService.BulkInsertIfNotExistsAsync(schedules);
        await schedulesRepo.DeleteByIdsAsync(schedules.Select(s => s.Id).ToList());

        return OnTickResult.Success(this);
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(30);
}
