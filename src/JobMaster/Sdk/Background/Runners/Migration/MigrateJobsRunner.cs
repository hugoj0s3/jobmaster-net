using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.Migration;

/// <summary>
/// Continuously forwards <c>OnMaster</c> (held) jobs on a <c>ClusterMode.Migrating</c> or
/// <c>ClusterMode.Archived</c> cluster to its <c>TargetActiveClusterId</c> cluster (on Archived: jobs that
/// landed after the switch; no target there is logged Critical). Nothing else ever advances an
/// <c>OnMaster</c> job on these clusters (<see cref="JobAndRecurringScheduleLifeCycleControl.AssignJobsToBucketsRunner"/>
/// only runs on <c>Active</c> clusters), so this runner has the field to itself. Uses the same
/// partition-lock acquire primitive as <c>AssignJobsToBucketsRunner</c>, so concurrent Coordinators on
/// the same cluster naturally split the work. If the target cluster isn't reachable, nothing is
/// deleted here — jobs simply stay held until the next tick, since (unlike purging) there's no TTL
/// forcing a decision.
/// </summary>
internal sealed class MigrateJobsRunner : JobMasterRunner
{
    private readonly IMasterClusterConfigurationService clusterConfigService;
    private readonly IMasterJobsService masterJobsService;
    private readonly IMasterJobsRepository jobsRepo;
    private readonly IMasterLogsRepository logsRepo;

    public MigrateJobsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        clusterConfigService = backgroundAgentWorker.GetClusterAwareService<IMasterClusterConfigurationService>();
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        jobsRepo = backgroundAgentWorker.GetClusterAwareRepository<IMasterJobsRepository>();
        logsRepo = backgroundAgentWorker.GetClusterAwareRepository<IMasterLogsRepository>();
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (BackgroundAgentWorker.StopRequested)
            return OnTickResult.Skipped(this);

        var cfg = clusterConfigService.Get();
        if (cfg == null)
            return OnTickResult.Skipped(this);

        var startTimeUtc = DateTime.UtcNow;
        var criteria = new JobQueryCriteria
        {
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            Status = JobMasterJobStatus.OnMaster,
            SortBy = new SortByCriteria { Property = nameof(JobRawModel.NextPlanExecutionAt), Ascending = true },
        };

        var probe = await masterJobsService.ProbeForAcquireAsync(criteria);
        if (probe.Count <= 0)
            return OnTickResult.Skipped(this);

        var isArchived = cfg.ClusterMode == ClusterMode.Archived;
        if (string.IsNullOrEmpty(cfg.TargetActiveClusterId))
        {
            if (isArchived)
            {
                logger.Critical(
                    $"Archived cluster '{BackgroundAgentWorker.ClusterConnConfig.ClusterId}' holds {probe.Count} job(s) but has no cluster to move them to " +
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
                "Held jobs remain on master until this is resolved — nothing is lost.";
            if (isArchived)
                logger.Critical(message, JobMasterLogCategory.Cluster, BackgroundAgentWorker.ClusterConnConfig.ClusterId);
            else
                logger.Error(message, JobMasterLogCategory.Cluster, BackgroundAgentWorker.ClusterConnConfig.ClusterId);
            return OnTickResult.Skipped(this);
        }

        var jobs = await masterJobsService.AcquireAndFetchAsync(criteria, startTimeUtc.Add(JobMasterConstants.DurationToLockRecords));
        if (jobs.Count == 0)
            return OnTickResult.Skipped(this);

        var intakeService = targetFactory.GetComponent<IMasterJobIntakeService>();
        var jobIds = jobs.Select(j => j.Id).ToList();
        var executions = await jobsRepo.QueryJobExecutionsForJobsAsync(jobIds);
        // "N" format (no dashes) -- matches how JobMasterLoggerExtensions stores ReferenceId for every
        // JobExecution-category log (referenceId.ToString("N")).
        var jobLogs = await logsRepo.QueryForReferenceIdsAsync(
            JobMasterLogCategory.JobExecution, jobIds.Select(id => id.ToString("N")).ToList());

        await intakeService.BulkInsertIfNotExistsAsync(jobs, executions, jobLogs);
        await jobsRepo.DeleteByIdsAsync(jobIds);
        if (jobLogs.Count > 0)
        {
            await logsRepo.DeleteByIdsAsync(jobLogs.Select(l => l.Id).ToList());
        }

        return OnTickResult.Success(this);
    }

    // Archived clusters only get the odd late job; Migrating ones are draining real traffic.
    public override TimeSpan SucceedInterval =>
        clusterConfigService.Get()?.ClusterMode == ClusterMode.Archived ? TimeSpan.FromMinutes(2.5) : TimeSpan.FromSeconds(30);
}
