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
/// Continuously forwards <c>OnMaster</c> (held) jobs on a <c>ClusterMode.Migrating</c> cluster to its
/// <c>TargetActiveClusterId</c> cluster. Nothing else ever advances an <c>OnMaster</c> job on a
/// Migrating cluster (<see cref="JobAndRecurringScheduleLifeCycleControl.AssignJobsToBucketsRunner"/>
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
        if (cfg == null || string.IsNullOrEmpty(cfg.TargetActiveClusterId))
            return OnTickResult.Skipped(this);

        var targetFactory = JobMasterClusterAwareComponentFactories.TryGetFactory(cfg.TargetActiveClusterId!);
        if (targetFactory == null)
        {
            logger.Error(
                $"Target active cluster '{cfg.TargetActiveClusterId}' is not reachable from this process. " +
                "Held jobs remain on master until this is resolved — nothing is lost.",
                JobMasterLogCategory.Cluster,
                BackgroundAgentWorker.ClusterConnConfig.ClusterId);
            return OnTickResult.Skipped(this);
        }

        var startTimeUtc = DateTime.UtcNow;
        var criteria = new JobQueryCriteria
        {
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            Status = JobMasterJobStatus.OnMaster,
            SortBy = new SortByCriteria { Property = nameof(JobRawModel.NextPlanExecutionAt), Ascending = true },
        };

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

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(30);
}
