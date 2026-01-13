using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Models;
using JobMaster.Contracts.Models.Attributes;
using JobMaster.Contracts.RecurrenceExpressions;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;
using JobMaster.Sdk.Contracts.Services;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services;

public class RecurringSchedulePlanner : JobMasterClusterAwareComponent, IRecurringSchedulePlanner
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
            logger.Debug($"Skipping: Status is {scheduleRawModel.Status}, not Active", JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
            return;
        }
        
        if (scheduleRawModel.IsStaticIdle(jobMasterRuntime.StartingAt))
        {
            logger.Debug("Skipping: Schedule is in static idle period", JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
            return;
        }
        
        if (scheduleRawModel.EndBefore.HasValue && scheduleRawModel.EndBefore.Value < DateTime.UtcNow)
        {
            logger.Debug($"Skipping: EndBefore ({scheduleRawModel.EndBefore:O}) is in the past", JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
            scheduleRawModel.TryEnded();
            await masterRecurringSchedulesService.UpsertAsync(scheduleRawModel);
            return;
        }
        
        if (scheduleRawModel.StartAfter.HasValue && scheduleRawModel.StartAfter.Value > DateTime.UtcNow)
        {
            logger.Debug($"Skipping: StartAfter ({scheduleRawModel.StartAfter:O}) is in the future", JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
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
            logger.Debug("Failed to acquire lock for recurring schedule planning", JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
            return;
        }
        
        logger.Debug("Lock acquired, starting to plan next dates", JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
        
        var recurringSchedule = RecurringScheduleConvertUtil.ToRecurringSchedule(scheduleRawModel);
        var handlerType = JobMasterDefinitionIdAttribute.GetJobHandlerTypeFromId(recurringSchedule.JobDefinitionId);
        if (handlerType == null)
        {
            throw new KeyNotFoundException($"Job handler type not found for ID: {recurringSchedule.JobDefinitionId}");
        }

        var baseDateTime = scheduleRawModel.LastPlanCoverageUntil ?? scheduleRawModel.StartAfter ?? scheduleRawModel.CreatedAt;
        logger.Debug($"Planning from baseDateTime={baseDateTime:O}, LastPlanCoverageUntil={scheduleRawModel.LastPlanCoverageUntil:O}, StartAfter={scheduleRawModel.StartAfter:O}, CreatedAt={scheduleRawModel.CreatedAt:O}", 
            JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
        
        var (lastPlanCoverageUntilUtc, nextDates, planningHorizonUsed) = PlanNextDates(
            recurringSchedule.Id, 
            scheduleRawModel.HasFailedOnLastPlanExecution ?? false,
            masterConfig?.IanaTimeZoneId ?? TimeZoneUtils.GetLocalIanaTimeZoneId(),
            recurringSchedule.RecurExpression, 
            timeToScheduleInAdvance, 
            baseDateTime, 
            scheduleRawModel.EndBefore);
        
        logger.Debug($"PlanNextDates returned {nextDates.Count} dates. lastPlanCoverageUntilUtc={lastPlanCoverageUntilUtc:O}, planningHorizonUsed={planningHorizonUsed:O}", 
            JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
        
        if (nextDates.IsNullOrEmpty())
        {
            // Check if schedule has permanently ended (e.g., reached end date)
            var ianaTimeZoneId = masterConfig?.IanaTimeZoneId ?? TimeZoneUtils.GetLocalIanaTimeZoneId();
            var checkTime = lastPlanCoverageUntilUtc ?? planningHorizonUsed;
            
            if (recurringSchedule.RecurExpression.HasEnded(checkTime, ianaTimeZoneId))
            {
                logger.Info($"Recurring schedule has ended. Marking as Completed. LastPlanCoverageUntil={checkTime:O}", 
                    JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
                
                scheduleRawModel.Status = RecurringScheduleStatus.Completed;
                scheduleRawModel.HasFailedOnLastPlanExecution = false;
                scheduleRawModel.LastPlanCoverageUntil = checkTime;
                scheduleRawModel.LastExecutedPlan = DateTime.UtcNow;
                await masterRecurringSchedulesService.UpsertAsync(scheduleRawModel);
                
                masterDistributedLockerService.ReleaseLock(lockKeys.RecurringSchedulePlan(scheduleRawModel.Id), lockToken);
                return;
            }
            
            logger.Warn($"No next dates to schedule in current window. Updating LastPlanCoverageUntil to {checkTime:O}", 
                JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
            
            // Update LastPlanCoverageUntil even when empty to prevent infinite loop
            // Schedule hasn't ended, just no occurrences in this planning window
            scheduleRawModel.HasFailedOnLastPlanExecution = false;
            scheduleRawModel.LastPlanCoverageUntil = checkTime;
            scheduleRawModel.LastExecutedPlan = DateTime.UtcNow;
            await masterRecurringSchedulesService.UpsertAsync(scheduleRawModel);
            
            masterDistributedLockerService.ReleaseLock(lockKeys.RecurringSchedulePlan(scheduleRawModel.Id), lockToken);
            return;
        }

        var jobs = new List<JobRawModel>();
        foreach (var nextDate in nextDates)
        {
            var job = NewJobRawModel(scheduleRawModel, handlerType, nextDate, masterConfig);
            jobs.Add(job);
        }
        
        logger.Debug($"Bulk scheduling {jobs.Count} jobs", JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);

        try
        {
            await scheduler.BulkScheduleAsync(jobs);
            logger.Debug($"Successfully bulk scheduled {jobs.Count} jobs", JobMasterLogSubjectType.RecurringSchedule, scheduleRawModel.Id);
        }
        catch (Exception e)
        {
            logger.Error("Recurring Schedule error", exception: e);
            
            scheduleRawModel.HasFailedOnLastPlanExecution = true;
            scheduleRawModel.LastExecutedPlan = DateTime.UtcNow;
            
            await masterRecurringSchedulesService.UpsertAsync(scheduleRawModel);
            
            masterDistributedLockerService.ReleaseLock(lockKeys.RecurringSchedulePlan(scheduleRawModel.Id), lockToken);
            return;
        }
       
        scheduleRawModel.HasFailedOnLastPlanExecution = false;
        scheduleRawModel.LastPlanCoverageUntil = lastPlanCoverageUntilUtc;
        scheduleRawModel.LastExecutedPlan = lastPlanCoverageUntilUtc;
        await masterRecurringSchedulesService.UpsertAsync(scheduleRawModel);
        
        masterDistributedLockerService.ReleaseLock(lockKeys.RecurringSchedulePlan(scheduleRawModel.Id), lockToken);
    }
    

    private JobRawModel NewJobRawModel(RecurringScheduleRawModel rawModel, Type handlerType, DateTime scheduledAt, ClusterConfigurationModel? config)
    {
        var recurringSchedule = RecurringScheduleConvertUtil.ToRecurringSchedule(rawModel);
        return Job.FromRecurringSchedule(rawModel.ClusterId, handlerType, recurringSchedule, scheduledAt, masterConfig: config).ToModel();
    }
    
    private static readonly int MaxOccurrencesPerRun = (int)(JobMasterConstants.MaxRunnerInterval.TotalSeconds * 1.5);
    private static readonly TimeSpan MinInterval = TimeSpan.FromSeconds(1);


    private static long ToSec(DateTime dt) => dt.Ticks / TimeSpan.TicksPerSecond;

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
        if (endBeforeUtc.HasValue && endBeforeUtc.Value < stopAt)
            stopAt = endBeforeUtc.Value;

        // When last plan failed: fetch already scheduled jobs in [baseDateTime, stopAt] and
        // build a seconds-level HashSet to skip duplicates within ±1s tolerance.
        HashSet<long>? scheduledSecs = null;
        DateTime? lastJobScheduledAt = null;
        if (hasFailedOnLastPlan)
        {
            var jobs = masterJobsService.Query(new JobQueryCriteria
            {
                RecurringScheduleId = recurringScheduleId,
                ScheduledFrom = baseDateTime,
                ScheduledTo = stopAt,
            });

            // Normalize scheduled times to seconds
            scheduledSecs = new HashSet<long>(jobs.Select(j => ToSec(j.OriginalScheduledAt)));
            lastJobScheduledAt = jobs.Max(x => x.OriginalScheduledAt);
        }

        var results = new List<DateTime>();
        var cursor = baseDateTime;

        for (int i = 0; i < MaxOccurrencesPerRun && cursor <= stopAt; i++)
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

            // Respect horizon/end bound
            if (at > stopAt) break;

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
}