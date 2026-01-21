using JobMaster.Sdk.Abstractions;

namespace JobMaster.Sdk.Background.ScanPlans;

internal static class ScanPlanner
{
    private const int LaneWidth = 1024;

    /// <summary>
    /// Calculates an optimized scan plan for job processing based on current system load.
    ///
    /// The algorithm:
    /// 1. Uses a fixed batch size provided by the client
    /// 2. Aims to process all pending work within half of the transient threshold
    /// 3. Starts with the highest allowed interval (capped at 10 minutes)
    /// 4. If current worker count can handle the load at this interval:
    ///    - Uses the interval
    ///    - Reduces batch size by 1.5x (if possible) to improve distribution
    /// 5. If workers are insufficient:
    ///    - Calculates the minimum interval needed to process work with available workers
    /// 
    /// Locking strategy:
    /// - Oversubscribes lockers (1.2x if enough workers, 1.5x otherwise)
    /// - Clamps locker count between 2 and 1024
    /// - Maps lockers to the specified lane to avoid conflicts
    /// </summary>
    internal static ScanPlanResult ComputeScanPlanHalfWindow(
        long backlog,
        int actualWorkers,
        int batchSize,
        TimeSpan transientThreshold,
        int lockerLane = 0,
        int minBatch = 50)
    {
        var minInterval = TimeSpan.FromSeconds(10);
        var maxInterval = transientThreshold < JobMasterConstants.MaxRunnerInterval ? transientThreshold : JobMasterConstants.MaxRunnerInterval;

        if (backlog <= 0 || actualWorkers <= 0 || batchSize <= 0)
        {
            int idleStart = lockerLane * LaneWidth + 1;
            return new ScanPlanResult
            {
                BatchSize = Math.Max(1, batchSize),
                Interval  = maxInterval,
                LockerMin = idleStart,
                LockerMax = idleStart
            };
        }

        // Target window = HALF of transient threshold
        var targetWindow = TimeSpan.FromTicks(transientThreshold.Ticks / 2);

        // Highest allowed interval (cap 10m, floor 10s)
        var idealInterval = targetWindow < maxInterval ? targetWindow : maxInterval;
        if (idealInterval < minInterval)
            idealInterval = minInterval;

        // Work in seconds (min interval = 10s)
        int scansAtIdeal = Math.Max(1, (int)Math.Floor(targetWindow.TotalSeconds / Math.Max(idealInterval.TotalSeconds, double.Epsilon)));

        // Workers needed to finish within the target at the ideal interval
        int workersNeeded = (int)Math.Ceiling(
            backlog / Math.Max(1.0, batchSize * (double)scansAtIdeal));

        TimeSpan interval;
        double lockerCount;
        int finalBatch = Math.Max(minBatch, batchSize);

        if (workersNeeded <= actualWorkers)
        {
            interval = idealInterval;

            // Reduce batch by 1.5× but ensure not too low
            int reduced = (int)Math.Ceiling(batchSize / 1.5);
            int sufficientWithHeadroom = finalBatch;

            if (workersNeeded == 1)
            {
                long denom = Math.Max(1, (long)actualWorkers * scansAtIdeal);
                var exactSufficient = (int)Math.Ceiling(backlog / (double)denom);
                sufficientWithHeadroom = (int)Math.Ceiling(exactSufficient * 1.20);
            }

            int candidate = Math.Max(reduced, sufficientWithHeadroom);
            finalBatch = Clamp(candidate, minBatch, batchSize);

            lockerCount = Math.Max(2, Math.Ceiling(workersNeeded * 1.2)); // +20%
        }
        else
        {
            long capacityPerScan = (long)actualWorkers * finalBatch;
            long scansNeeded = (long)Math.Ceiling(backlog /
                                Math.Max(1.0, (double)capacityPerScan));

            interval = scansNeeded <= 0
                ? maxInterval
                : TimeSpan.FromTicks(
                    Math.Max(1, targetWindow.Ticks / Math.Max(1L, scansNeeded)));

            if (interval < minInterval) interval = minInterval;
            if (interval > maxInterval) interval = maxInterval;

            lockerCount = Math.Max(2, Math.Ceiling(actualWorkers * 1.5)); // +50%
        }

        // Map lockers into lane: [lane*1024+1 .. +count-1]
        int count = Math.Max(2, (int)Math.Min(LaneWidth, Math.Ceiling(lockerCount)));
        int start = lockerLane * LaneWidth + 1;
        int end   = start + count - 1;

        return new ScanPlanResult
        {
            BatchSize = finalBatch,
            Interval  = interval,
            LockerMin = start,
            LockerMax = end
        };
    }
    
    private static int Clamp(int value, int min, int max)
    {
        if (value < min) return min;
        if (value > max) return max;
        return value;
    }
}
