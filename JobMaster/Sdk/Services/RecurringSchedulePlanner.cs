using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.RecurrenceExpressions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Services;

internal class RecurringSchedulePlanner : JobMasterClusterAwareComponent, IRecurringSchedulePlanner
{
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;
    private readonly IJobMasterSchedulerClusterAware scheduler;
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly IMasterJobsService masterJobsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly IJobMasterRuntime jobMasterRuntime;
    private readonly IJobMasterLogger logger;
    private JobMasterLockKeys lockKeys;
    public RecurringSchedulePlanner(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterClusterConfigurationService masterClusterConfigurationService,
        IJobMasterSchedulerClusterAware scheduler,
        IMasterRecurringSchedulesService masterRecurringSchedulesService,
        IMasterJobsService masterJobsService,
        IMasterDistributedLockerService masterDistributedLockerService,
        IJobMasterRuntime jobMasterRuntime,
        IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.masterClusterConfigurationService = masterClusterConfigurationService;
        this.scheduler = scheduler;
        this.masterRecurringSchedulesService = masterRecurringSchedulesService;
        this.masterJobsService = masterJobsService;
        this.masterDistributedLockerService = masterDistributedLockerService;
        this.jobMasterRuntime = jobMasterRuntime;
        this.logger = logger;
        
        lockKeys = new JobMasterLockKeys(clusterConnConfig.ClusterId);
    }

    public async Task ScheduleNextJobsAsync(RecurringScheduleRawModel scheduleRawModel)
    {
        if (scheduleRawModel.Status != RecurringScheduleStatus.Active)
        {
            logger.Debug($"Skipping: Status is {scheduleRawModel.Status}, not Active", JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
            return;
        }
        
        if (scheduleRawModel.IsStaticIdle(jobMasterRuntime.StartingAt))
        {
            logger.Debug("Skipping: Schedule is in static idle period", JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
            return;
        }
        
        if (scheduleRawModel.EndBefore.HasValue && scheduleRawModel.EndBefore.Value < DateTime.UtcNow)
        {
            logger.Debug($"Skipping: EndBefore ({scheduleRawModel.EndBefore:O}) is in the past", JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
            scheduleRawModel.TryEnded();
            await masterRecurringSchedulesService.UpdateAsync(scheduleRawModel);
            return;
        }
        
        if (scheduleRawModel.StartAfter.HasValue && scheduleRawModel.StartAfter.Value > DateTime.UtcNow)
        {
            logger.Debug($"Skipping: StartAfter ({scheduleRawModel.StartAfter:O}) is in the future", JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
            return;
        }
        
        var masterConfig = masterClusterConfigurationService.Get();
        var timeToScheduleInAdvance = masterConfig?.TransientThreshold ?? JobMasterConstants.DurationToLockRecords;
        if (timeToScheduleInAdvance < JobMasterConstants.DurationToLockRecords) 
        {
            timeToScheduleInAdvance = JobMasterConstants.DurationToLockRecords;
        }
        
        var lockToken = masterDistributedLockerService.TryLock(lockKeys.RecurringSchedulePlan(scheduleRawModel.Id), timeToScheduleInAdvance);
        if (lockToken == null)
        {
            logger.Debug("Failed to acquire lock for recurring schedule planning", JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
            return;
        }
        
        logger.Debug("Lock acquired, starting to plan next dates", JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
        
        var recurringSchedule = RecurringScheduleConvertUtil.ToRecurringSchedule(scheduleRawModel);
        var handlerType = JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId(recurringSchedule.JobDefinitionId);
        if (handlerType == null)
        {
            logger.Critical($"Job handler type not found for ID: {recurringSchedule.JobDefinitionId} for recurring schedule {scheduleRawModel.Id}");
            
            // Avoid keep get this schedule again and again. Delay the next attempt.
            scheduleRawModel.LastPlanCoverageUntil = DateTime.UtcNow.Add(JobMasterConstants.DurationToLockRecords);
            await UpdateAndReleasePlanLockAsync(scheduleRawModel, lockToken);
            
            return;
        }

        var baseDateTime = scheduleRawModel.LastPlanCoverageUntil ?? scheduleRawModel.StartAfter ?? scheduleRawModel.CreatedAt;
        logger.Debug($"Planning from baseDateTime={baseDateTime:O}, LastPlanCoverageUntil={scheduleRawModel.LastPlanCoverageUntil:O}, StartAfter={scheduleRawModel.StartAfter:O}, CreatedAt={scheduleRawModel.CreatedAt:O}", 
            JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
        
        var (lastPlanCoverageUntilUtc, nextDates, planningHorizonUsed) = PlanNextDates(
            recurringSchedule.Id,
            scheduleRawModel.HasFailedOnLastPlanExecution ?? false,
            masterConfig?.IanaTimeZoneId ?? TimeZoneUtils.GetLocalIanaTimeZoneId(),
            recurringSchedule.RecurExpression,
            timeToScheduleInAdvance,
            baseDateTime,
            scheduleRawModel.EndBefore);

        logger.Debug($"PlanNextDates returned {nextDates.Count} dates. lastPlanCoverageUntilUtc={lastPlanCoverageUntilUtc:O}, planningHorizonUsed={planningHorizonUsed:O}",
            JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);

        // Empty now only ever means either the recurrence has genuinely run out of occurrences, or its
        // explicit EndBefore (still in the future -- already-passed EndBefore is caught earlier, at the
        // top of this method) clamped the horizon before the next real occurrence -- PlanNextDates
        // always returns at least one occurrence otherwise, even when it's beyond this pass's horizon
        // (see its own remarks).
        if (nextDates.IsNullOrEmpty())
        {
            var ianaTimeZoneId = masterConfig?.IanaTimeZoneId ?? TimeZoneUtils.GetLocalIanaTimeZoneId();
            var checkTime = lastPlanCoverageUntilUtc ?? planningHorizonUsed;

            // Advance the watermark either way -- no need to re-derive the exact same empty result
            // again next tick. In the EndBefore-clamped case this stably pins LastPlanCoverageUntil at
            // EndBefore itself (GetNextOccurrence(EndBefore) still lands beyond the still-EndBefore-
            // clamped horizon on the following attempt, so it just holds here) until EndBefore actually
            // passes and the check at the top of this method takes over.
            scheduleRawModel.HasFailedOnLastPlanExecution = false;
            scheduleRawModel.LastPlanCoverageUntil = checkTime;
            scheduleRawModel.LastExecutedPlan = DateTime.UtcNow;

            if (recurringSchedule.RecurExpression.HasEnded(checkTime, ianaTimeZoneId))
            {
                logger.Info($"Recurring schedule has ended. Marking as Completed. LastPlanCoverageUntil={checkTime:O}",
                    JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
                scheduleRawModel.Status = RecurringScheduleStatus.Completed;
            }
            else
            {
                logger.Warn($"No next dates to schedule in current window (EndBefore clamp). LastPlanCoverageUntil advanced to {checkTime:O}.",
                    JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
            }

            await UpdateAndReleasePlanLockAsync(scheduleRawModel, lockToken);
            return;
        }

        var jobs = new List<JobRawModel>();
        foreach (var nextDate in nextDates)
        {
            var job = NewJobRawModel(scheduleRawModel, handlerType, nextDate, masterConfig);
            jobs.Add(job);
        }
        
        logger.Debug($"Bulk scheduling {jobs.Count} jobs", JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);

        try
        {
            await scheduler.BulkScheduleAsync(jobs);
            logger.Debug($"Successfully bulk scheduled {jobs.Count} jobs", JobMasterLogCategory.RecurringSchedule, scheduleRawModel.Id);
        }
        catch (Exception e)
        {
            logger.Error("Recurring Schedule error", exception: e);
            
            scheduleRawModel.HasFailedOnLastPlanExecution = true;
            scheduleRawModel.LastExecutedPlan = DateTime.UtcNow;
            
            await UpdateAndReleasePlanLockAsync(scheduleRawModel, lockToken);
            return;
        }
       
        scheduleRawModel.HasFailedOnLastPlanExecution = false;
        scheduleRawModel.LastPlanCoverageUntil = lastPlanCoverageUntilUtc;
        scheduleRawModel.LastExecutedPlan = lastPlanCoverageUntilUtc;
        await UpdateAndReleasePlanLockAsync(scheduleRawModel, lockToken);
    }

    internal (DateTime? lastSchedule, IList<DateTime> nextDates, DateTime planningHorizon) PlanNextDates(
        Guid recurringScheduleId,
        bool hasFailedOnLastPlan,
        string ianaTimeZoneId,
        IRecurrenceCompiledExpr expr,
        TimeSpan horizon,
        DateTime baseDateTime,
        DateTime? endBeforeUtc)
    {
        var stopAt = DateTime.UtcNow + horizon;

        // Only an explicit EndBefore clamp forbids the "always at least one" fallback below (materializing
        // a job past the schedule's own declared end would be wrong) -- a plain horizon clamp doesn't.
        var stopAtIsExplicitEnd = false;
        if (endBeforeUtc.HasValue && endBeforeUtc.Value < stopAt)
        {
            stopAt = endBeforeUtc.Value;
            stopAtIsExplicitEnd = true;
        }

        // When last plan failed: fetch already scheduled jobs in [baseDateTime, stopAt] and
        // build a seconds-level HashSet to skip duplicates within ±1s tolerance.
        HashSet<long>? scheduledSecs = null;
        DateTime? lastJobScheduledAt = null;
        if (hasFailedOnLastPlan)
        {
            var jobs = masterJobsService.Query(new JobQueryCriteria
            {
                SourceId = recurringScheduleId,
                TriggerSourceTypes = new List<JobMasterTriggerSourceType>
                {
                    JobMasterTriggerSourceType.StaticRecurring,
                    JobMasterTriggerSourceType.DynamicRecurring
                },
                NextPlanExecutionAtFrom = baseDateTime,
                NextPlanExecutionAtTo = stopAt,
            });

            // Normalize scheduled times to seconds
            scheduledSecs = new HashSet<long>(jobs.Select(j => ToSec(j.ScheduledAt)));
            lastJobScheduledAt = jobs.Max(x => x.ScheduledAt);
        }

        var results = new List<DateTime>();
        var cursor = baseDateTime;

        // No "cursor <= stopAt" loop guard -- unlike before, we need at least one GetNextOccurrence
        // call to happen even when baseDateTime already starts beyond stopAt, so the "always at least
        // one" rule below can apply. Every iteration still terminates via an internal break (occurrence
        // exhausted, occurrence beyond stopAt, or the progress guard), bounded by MaxOccurrencesPerRun
        // regardless.
        for (int i = 0; i < MaxOccurrencesPerRun; i++)
        {
            var cursorInTheTimeZone = TimeZoneUtils.ConvertUtcToDateTimeTz(cursor, ianaTimeZoneId);
            var nextInTimeZone = expr.GetNextOccurrence(cursorInTheTimeZone, ianaTimeZoneId);
            if (!nextInTimeZone.HasValue) break;

            var next = TimeZoneUtils.ConvertDateTimeTzToUtc(nextInTimeZone.Value, ianaTimeZoneId);

            // If we have prior scheduled items (due to a failed plan), skip
            // occurrences that are within ±1s of already-scheduled dates.
            if (scheduledSecs != null)
            {
                var nextSec = ToSec(next);
                if (scheduledSecs.Contains(nextSec) ||
                    scheduledSecs.Contains(nextSec - 1) ||
                    scheduledSecs.Contains(nextSec + 1))
                {
                    // Move cursor forward and continue generating
                    cursor = next;
                    continue;
                }
            }

            var at = next;

            // Enforce minimum 1s spacing (we support but discourage sub-second cadence)
            if (i > 0 && at <= cursor + MinInterval)
                at = cursor + MinInterval;

            if (at > stopAt)
            {
                // Always return at least one occurrence rather than leaving results empty just because
                // the recurrence's own cadence exceeds this pass's horizon (e.g. an "every year" schedule
                // against a horizon measured in minutes) -- the job gets materialized/dispatched right
                // away regardless of how far out it is; if the schedule is later cancelled, this job is
                // cancelled along with it like any other. Only withheld when the horizon was clamped by
                // the schedule's own explicit EndBefore, since materializing a job past a declared end
                // would contradict it -- that case still correctly falls through to empty, letting the
                // HasEnded check in ScheduleNextJobsAsync mark the schedule Completed instead.
                if (results.Count == 0 && !stopAtIsExplicitEnd)
                {
                    results.Add(at);
                }
                break;
            }

            // Progress guard (if expression doesn’t advance and clamp didn’t either)
            if (i > 0 && at == cursor)
                break;

            results.Add(at);

            // Rely on expression to advance; we move the cursor to the accepted (possibly clamped) time
            cursor = at;
        }

        DateTime? lastScheduleAt = null;
        if (results.Any())
        {
            lastScheduleAt = results.Max();
        }

        lastScheduleAt ??= lastJobScheduledAt;

        if (lastJobScheduledAt.HasValue &&
            lastScheduleAt.HasValue &&
            lastJobScheduledAt.Value > lastScheduleAt.Value)
        {
            lastScheduleAt = lastJobScheduledAt.Value;
        }

        return (lastScheduleAt, results, stopAt);
    }
    
    private async Task UpdateAndReleasePlanLockAsync(RecurringScheduleRawModel scheduleRawModel, string lockToken)
    {
        try
        {
            await masterRecurringSchedulesService.UpdateAsync(scheduleRawModel);
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKeys.RecurringSchedulePlan(scheduleRawModel.Id), lockToken);
        }
    }

    private JobRawModel NewJobRawModel(RecurringScheduleRawModel rawModel, Type handlerType, DateTime scheduledAt, ClusterConfigurationModel? config)
    {
        var recurringSchedule = RecurringScheduleConvertUtil.ToRecurringSchedule(rawModel);
        return Job.FromRecurringSchedule(rawModel.ClusterId, handlerType, recurringSchedule, scheduledAt, masterConfig: config).ToModel();
    }
    
    private static readonly int MaxOccurrencesPerRun = (int)(JobMasterConstants.MaxRunnerInterval.TotalSeconds * 1.5);
    private static readonly TimeSpan MinInterval = TimeSpan.FromSeconds(1);


    private static long ToSec(DateTime dt) => dt.Ticks / TimeSpan.TicksPerSecond;
}