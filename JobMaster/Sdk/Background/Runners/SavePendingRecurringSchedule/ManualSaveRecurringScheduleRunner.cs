using System.Collections.Concurrent;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Background.Runners;
using JobMaster.Sdk.Abstractions.Background.SavePendingRecurringSchedules;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.SavePendingRecurringSchedule;

/// <summary>
/// Processes <c>PendingSave</c> recurring schedules queued for a specific bucket.
/// Must be activated via <see cref="DefineBucketId"/> before ticking; skips immediately
/// if no bucket ID is set or the bucket is not in an Active or Completing state.
/// For each pulled schedule, delegates to <see cref="RecurringScheduleSavePendingOperation"/>
/// to persist or route it. On per-schedule failure the schedule is re-queued and a
/// consecutive-failure backoff (10–60 s) is applied.
/// Runs every <see cref="SucceedInterval"/> while the bucket is healthy.
/// </summary>
internal class ManualSaveRecurringScheduleRunner : BucketAwareRunner, ISaveRecurringSchedulerRunner
{
    private readonly TimeSpan interval = TimeSpan.FromSeconds(2.5);
    private int failedSavedCountConsecutive = 0;
    

    private readonly IAgentJobsDispatcherService agentJobsDispatcherService;
    private readonly IMasterBucketsService masterBucketsService;
    private readonly RecurringScheduleSavePendingOperation savePendingOperation;
    
    public ManualSaveRecurringScheduleRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker)
    {
        agentJobsDispatcherService = backgroundAgentWorker.GetClusterAwareService<IAgentJobsDispatcherService>();
        masterBucketsService = backgroundAgentWorker.GetClusterAwareService<IMasterBucketsService>();
        savePendingOperation = new RecurringScheduleSavePendingOperation(backgroundAgentWorker);
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
        var recurringSchedules = await agentJobsDispatcherService.PullSavePendingRecurAsync(
            BackgroundAgentWorker.AgentConnectionId, 
            BucketId!, 
            BackgroundAgentWorker.BucketBufferSize);

        if (recurringSchedules.Count <= 0)
        {
            return OnTickResult.Skipped(TimeSpan.FromSeconds(1));
        }
        
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
            // 2. Parallel Processing
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
                    
                    logger.Error("Failed to save recurring schedule", JobMasterLogCategory.RecurringSchedule, schedule.Id, exception: e);
                    // Failure: Attempt to re-queue immediately in memory
                    try 
                    {
                        await agentJobsDispatcherService.AddSavePendingRecurAsync(schedule);
                        pendingTracker.TryRemove(schedule.Id, out _);
                    }
                    catch (Exception e2)
                    {
                        logger.Critical(
                            $"Failed to add recurring schedule to queue recurring. Data: {schedule.ToLogSummary()}", 
                            JobMasterLogCategory.RecurringSchedule, schedule.Id, exception: e2);
                    }
                    
                    return;
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
                    logger.Critical($"Failed to add recurring schedule to queue. Data: {schedule.ToLogSummary()}", JobMasterLogCategory.RecurringSchedule, schedule.Id);
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
    
    private Task SaveRecurringScheduleAsync(RecurringScheduleRawModel recurringScheduleRawModel)
        => savePendingOperation.SaveRecurringScheduleAsync(recurringScheduleRawModel);
    

    public void DefineBucketId(string bucketId)
    {
        if (string.IsNullOrEmpty(bucketId))
            throw new ArgumentNullException(nameof(bucketId));
        
        if (!string.IsNullOrEmpty(BucketId))
            throw new InvalidOperationException("BucketId is already defined.");
        
        this.BucketId = bucketId;
    }
}
