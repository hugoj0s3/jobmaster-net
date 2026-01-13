using JobMaster.Contracts.Utils;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

public static class JobsDequeuePolicy
{
    /// <summary>
    /// Perform queue maintenance (abort timed-out, start queued) and decide whether to skip dequeueing this tick.
    /// Returns a tuple with (shouldSkip, countAvailability, abortedCount, hasStartedAny).
    /// </summary>
    public static (bool shouldSkip, int countAvailability, int abortedCount, bool hasStartedAny)
        PrepareAndShouldSkip<T>(TaskQueueControl<T> taskQueue)
    {
        var abortedCount = taskQueue.AbortTimeoutTasks();
        var hasStartedAny = taskQueue.StartQueuedTasksIfHasSlotAvailable();

        var countAvailability = taskQueue.CountAvailability();
        
        

        return (countAvailability <= 0, countAvailability, abortedCount, hasStartedAny);
    }
    
    private static bool ShouldSkipDequeueThisTick(
        int countAvailability,
        int waitingQueueCapacity,
        int countRunning,
        int countWaiting,
        bool hasStartedAny)
    {
        if (countAvailability <= 0)
        {
            return true;
        }

        // If there is no backlog waiting to start, prefer to dequeue more
        if (countWaiting == 0)
        {
            return false;
        }

        if (!hasStartedAny && countRunning > 0 && JobMasterRandomUtil.GetBoolean(0.75))
        {
            return true;
        }

        if (countAvailability < 10 && countRunning > 0)
        {
            return true;
        }

        if (countAvailability < 20 && waitingQueueCapacity >= 20 && countRunning > 0 && JobMasterRandomUtil.GetBoolean(0.25))
        {
            return true;
        }

        if (countAvailability < 30 && waitingQueueCapacity >= 30 && countRunning > 0 && JobMasterRandomUtil.GetBoolean(0.5))
        {
            return true;
        }

        if (countAvailability < 50 && waitingQueueCapacity <= 50 && countRunning > 0 && JobMasterRandomUtil.GetBoolean(0.25))
        {
            return true;
        }

        return false;
    }
}
