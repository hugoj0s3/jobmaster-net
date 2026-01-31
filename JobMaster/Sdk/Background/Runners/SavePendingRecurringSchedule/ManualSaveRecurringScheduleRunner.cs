using System.Collections.Concurrent;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;

namespace JobMaster.Sdk.Background.Runners.SavePendingRecurringSchedule;

internal class ManualSaveRecurringScheduleRunner : SaveRecurringSchedulerRunner
{
    private readonly TimeSpan interval = TimeSpan.FromSeconds(2.5);
    private int failedSavedCountConsecutive = 0;
    

    public ManualSaveRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
    }
    
    public override TimeSpan SucceedInterval => interval;

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        if (string.IsNullOrEmpty(BucketId))
        {
            return OnTickResult.Skipped(this);
        }
        
        var bucket = masterBucketsService.Get(BucketId!, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (bucket == null || (bucket.Status != BucketStatus.Active && bucket.Status != BucketStatus.Completing))
        {
            return OnTickResult.Skipped(this);
        }

        // 1. Dequeue batch
        var recurringSchedules = await agentJobsDispatcherService.DequeueSavePendingRecurAsync(
            BackgroundAgentWorker.AgentConnectionId, 
            BucketId!, 
            BackgroundAgentWorker.BatchSize);

        if (recurringSchedules.Count <= 0)
        {
            return OnTickResult.Skipped(TimeSpan.FromMilliseconds(interval.TotalMilliseconds * 5));
        }

        // 2. BURST MODE: Force acquire slots.
        // We force entry even if we slightly exceed limits to ensure persistence priority.
        int acquiredCount = recurringSchedules.Count;
        
        bool hasFailed = false;
        
        // Tracker to ensure no data loss in case of cancellation
        var pendingTracker = new ConcurrentDictionary<Guid, RecurringScheduleRawModel>(
            recurringSchedules.ToDictionary(x => x.Id, x => x)
        );

        using var batchTimeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(2.5));
        var parallelOptions = new ParallelOptions 
        { 
            MaxDegreeOfParallelism = 5,
            CancellationToken = batchTimeoutCts.Token 
        };

        try
        {
            // 3. Parallel Processing
            await JobMasterParallelUtil.ForEachAsync(recurringSchedules, parallelOptions, async (schedule, token) =>
            {
                try
                {
                    // Atomic Logic Block
                    // ------------------------------------------------
                    // Step A: Save the current state
                    await SaveRecurringScheduleAsync(schedule);
                    
                    // Success: Remove from tracker
                    pendingTracker.TryRemove(schedule.Id, out _);
                }
                catch (Exception e)
                {
                    hasFailed = true;
                    
                    logger.Error("Failed to save recurring schedule", JobMasterLogSubjectType.RecurringSchedule, schedule.Id, exception: e);
                    // Failure: Attempt to re-queue immediately in memory
                    try 
                    {
                        await agentJobsDispatcherService.AddSavePendingRecurAsync(schedule);
                        pendingTracker.TryRemove(schedule.Id, out _);
                    }
                    catch (Exception e2)
                    {
                        logger.Critical(
                            $"Failed to add recurring schedule to queue recurring. Data: {InternalJobMasterSerializer.Serialize(schedule)}", 
                            JobMasterLogSubjectType.RecurringSchedule, schedule.Id, exception: e2);
                    }
                    
                    return;
                }

                try
                {
                    // Step B: Schedule next jobs (only if Step A succeeds)
                    logger.Debug("Scheduling next jobs", JobMasterLogSubjectType.RecurringSchedule, schedule.Id);
                    await ScheduleNextJobs(schedule);
                    logger.Debug("Scheduled next jobs", JobMasterLogSubjectType.RecurringSchedule, schedule.Id);
                }
                catch (Exception e2)
                {
                    hasFailed = true;
                    logger.Error("Failed to schedule next jobs", JobMasterLogSubjectType.RecurringSchedule, schedule.Id, exception: e2);
                }
            });
        }
        catch (OperationCanceledException)
        {
            // 4. Rescue: Handle batch timeout or shutdown
            foreach (var schedule in pendingTracker.Values)
            {
                try
                {
                    await agentJobsDispatcherService.AddSavePendingRecurAsync(schedule);
                }
                catch
                {
                    // Log Critical
                }
            }
        }

        // 6. Backoff Logic
        if (hasFailed)
        {
            failedSavedCountConsecutive++;
            var secondsToWait = Math.Min(60, 10 + failedSavedCountConsecutive * 5);
            return OnTickResult.Skipped(TimeSpan.FromSeconds(secondsToWait));
        }
        
        failedSavedCountConsecutive = 0;
        return OnTickResult.Success(this);
    }
}