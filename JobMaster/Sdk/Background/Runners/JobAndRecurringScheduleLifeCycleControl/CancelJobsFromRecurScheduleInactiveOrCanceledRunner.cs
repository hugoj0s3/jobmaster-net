using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

/// <summary>
/// Bulk-cancels all pending jobs for recurring schedules that have been inactivated or
/// cancelled and have <c>IsJobCancellationPending</c> set. In-flight jobs are protected by
/// their partition lock and will be caught by the execution-time schedule validation inside
/// <c>JobsExecutionEngine</c>. The number of distributed lock slots scales with workload:
/// <c>ceil(count / transferBatchSize)</c>, so a single slot suffices when there is little
/// to cancel and multiple workers can run in parallel when the backlog is large.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class CancelJobsFromRecurScheduleInactiveOrCanceledRunner : JobMasterRunner
{
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly JobMasterLockKeys lockKeys;
    private readonly IMasterDistributedLockerService distributedLockerService;
    private readonly IMasterJobsService masterJobsService;

    public CancelJobsFromRecurScheduleInactiveOrCanceledRunner(
        IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        distributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        masterJobsService = backgroundAgentWorker.GetClusterAwareService<IMasterJobsService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        var utcNow = DateTime.UtcNow;
        var durationToLock = JobMasterConstants.DurationToLockRecords;
        var cutOffTime = utcNow.Add(durationToLock).AddSeconds(-30);

        var recurringScheduleQueryCriteria = new RecurringScheduleQueryCriteria()
        {
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            CanceledOrInactive = true,
            IsJobCancellationPending = true,
            Offset = 0,
            SortBy = new SortByCriteria()
            {
                Property = nameof(RecurringScheduleRawModel.LastPlanCoverageUntil),
                Ascending = false,
            }
        };

        var count = await masterRecurringSchedulesService.ProbeCountForAcquireAsync(recurringScheduleQueryCriteria);
        if (count <= 0)
        {
            return OnTickResult.Skipped(this);
        }

        var slotCount = (int)Math.Ceiling((double)count / BackgroundAgentWorker.TransferBatchSize);
        var lockSlot = JobMasterRandomUtil.GetInt(1, slotCount + 1);
        var lockToken = distributedLockerService.TryLock(lockKeys.RecurringSchedulerLock(lockSlot), durationToLock.Add(TimeSpan.FromMinutes(1)));
        if (lockToken == null)
        {
            return OnTickResult.Locked(SucceedInterval);
        }

        try
        {
            var recurringSchedules = await masterRecurringSchedulesService.AcquireAndFetchAsync(recurringScheduleQueryCriteria, utcNow.Add(durationToLock));
            if (recurringSchedules.Count <= 0)
            {
                return OnTickResult.Skipped(this);
            }

            foreach (var recurringSchedule in recurringSchedules)
            {
                if (cutOffTime <= DateTime.UtcNow)
                {
                    logger.Warn($"Runner timeout {durationToLock}", JobMasterLogCategory.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                    break;
                }

                if (ct.IsCancellationRequested)
                {
                    break;
                }

                await CencelJobsAsync(recurringSchedule, durationToLock, ct);
            }

            return OnTickResult.Success(this);
        }
        finally
        {
            distributedLockerService.ReleaseLock(lockKeys.RecurringSchedulerLock(lockSlot), lockToken);
        }
    }

    private async Task CencelJobsAsync(RecurringScheduleRawModel recurringScheduleRawModel, TimeSpan durationToLock, CancellationToken ct)
    {
        if (distributedLockerService.IsLocked(lockKeys.RecurringSchedulePlan(recurringScheduleRawModel.Id)))
        {
            return;
        }

        var jobQueryCriteria = new JobQueryCriteria()
        {
            CountLimit = BackgroundAgentWorker.TransferBatchSize,
            SourceId = recurringScheduleRawModel.Id,
            TriggerSourceTypes = new List<JobMasterTriggerSourceType>
            {
                recurringScheduleRawModel.RecurringScheduleType == RecurringScheduleType.Static
                    ? JobMasterTriggerSourceType.StaticRecurring
                    : JobMasterTriggerSourceType.DynamicRecurring
            },
            Offset = 0,
            SortBy = new SortByCriteria()
            {
                Property = nameof(JobRawModel.NextPlanExecutionAt),
                Ascending = true,
            },
        };

        if (ct.IsCancellationRequested)
        {
            return;
        }

        var expiresAtUtc = DateTime.UtcNow.Add(durationToLock);
        var jobs = await masterJobsService.AcquireAndFetchAsync(jobQueryCriteria, expiresAtUtc);
        var jobIdsToCancel = jobs
            .Where(x => !x.Status.IsFinalStatus())
            .Select(x => x.Id)
            .ToList();

        if (jobIdsToCancel.Count <= 0)
        {
            recurringScheduleRawModel.HasCancelJobsFinish();
            await masterRecurringSchedulesService.UpdateAsync(recurringScheduleRawModel);
            return;
        }

        var bulkUpdateRequest = BulkJobUpdateRequest.Cancel(jobIdsToCancel);
        await masterJobsService.BulkUpdateAsync(bulkUpdateRequest);

        recurringScheduleRawModel.HasCancelJobsFinish();
        await masterRecurringSchedulesService.UpdateAsync(recurringScheduleRawModel);
    }

    public override TimeSpan SucceedInterval => TimeSpan.FromSeconds(30);
}
