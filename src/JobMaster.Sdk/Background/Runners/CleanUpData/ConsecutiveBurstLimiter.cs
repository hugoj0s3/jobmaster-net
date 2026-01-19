namespace JobMaster.Sdk.Background.Runners.CleanUpData;

public sealed class ConsecutiveBurstLimiter
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
