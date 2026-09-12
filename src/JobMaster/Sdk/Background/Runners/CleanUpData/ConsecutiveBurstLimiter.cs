namespace JobMaster.Sdk.Background.Runners.CleanUpData;

/// <summary>
/// Controls the next-tick interval for cleanup runners that process data in batches.
/// Returns a shortened <c>burstNext</c> interval for up to <c>maxConsecutiveBursts</c>
/// consecutive full-batch deletions, then resets to the normal <c>desiredNext</c> interval.
/// This allows a runner to drain a large backlog quickly without permanently raising its
/// tick frequency.
/// </summary>
internal sealed class ConsecutiveBurstLimiter
{
    private readonly int maxConsecutiveBursts;
    private readonly int batchSize;
    private int consecutiveBurstCount;

    public ConsecutiveBurstLimiter(int maxConsecutiveBursts, int batchSize)
    {
        this.maxConsecutiveBursts = maxConsecutiveBursts;
        this.batchSize = batchSize;
        consecutiveBurstCount = 0;
    }

    public TimeSpan Next(TimeSpan desiredNext, TimeSpan burstNext, int rowsAffected)
    {
        if (rowsAffected >= batchSize)
        {
            if (consecutiveBurstCount + 1 >= maxConsecutiveBursts)
            {
                consecutiveBurstCount = 0;
                return desiredNext;
            }

            consecutiveBurstCount++;
            return burstNext;
        }

        consecutiveBurstCount = 0;
        return desiredNext;
    }
}
