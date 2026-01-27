using System.Diagnostics;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Background;

internal static class RunnerDelayUtil
{
    private const double FragmentDelayInSeconds = 2.5;
    
    public static readonly TimeSpan MaxTimeToReleaseSemaphore = TimeSpan.FromSeconds(15);
    
    internal static async Task<TimeSpan> DelayAsync(
        TimeSpan delay, 
        CancellationToken cancellationToken, 
        double earlyReleaseChance = 0)
    {
        if (delay < TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }
        
        // If the delay is 1 minute or less, use a single delay
        if (delay.TotalSeconds <= FragmentDelayInSeconds)
        {
            await Task.Delay(delay, cancellationToken);
            return delay;
        }
        
        var stopWatch = Stopwatch.StartNew();
        
        // Divide by delays of 10 seconds
        var countDelays = (int)(delay.TotalSeconds / FragmentDelayInSeconds);
        
        
        for (int i = 0; i < countDelays; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                stopWatch.Stop();
                return stopWatch.Elapsed;
            }
            
            await Task.Delay(TimeSpan.FromSeconds(FragmentDelayInSeconds), cancellationToken);

            if (earlyReleaseChance > 0 && JobMasterRandomUtil.GetBoolean(earlyReleaseChance))
            {
                return stopWatch.Elapsed;
            }
        }
        
        var lastDelayInMilliseconds = delay.TotalMilliseconds - TimeSpan.FromSeconds(countDelays * FragmentDelayInSeconds).TotalMilliseconds;
        if (cancellationToken.IsCancellationRequested)
        {
            return stopWatch.Elapsed;
        }
        
        if (lastDelayInMilliseconds > 0)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(lastDelayInMilliseconds), cancellationToken);
        }

        stopWatch.Stop();
        return stopWatch.Elapsed;
    }
    
    /// <summary>
    /// Calculates a random jitter delay to prevent thundering herd effects and reduce database lock contention.
    /// It uses a stochastic approach to break synchronization patterns between multiple runners.
    /// </summary>
    /// <param name="consecutiveFails">The number of consecutive failures to apply a soft exponential backoff.</param>
    /// <returns>A TimeSpan representing the calculated jitter delay.</returns>
    internal static TimeSpan CalcJitter(int consecutiveFails = 0)
    {
        // 1. Base Drift: A constant small noise (5ms to 50ms) 
        // to ensure threads finishing at the same time disperse immediately.
        int jitterMs = JobMasterRandomUtil.GetInt(5, 50);

        // 2. Anti-Sync Pattern Breaker:
        // 15% chance to apply a larger jump to break long-term synchronization cycles (harmonics).
        if (JobMasterRandomUtil.GetBoolean(0.15))
        {
            jitterMs += JobMasterRandomUtil.GetInt(100, 400);
        }

        // 3. Failure Penalty (Soft Exponential Backoff):
        // If the runner is failing, we increase the jitter to give the DB some breathing room.
        // Formula: $Jitter = Jitter + \min(2000, 2^{fails} \times 50)$
        if (consecutiveFails > 0)
        {
            var failurePenalty = Math.Min(2000, (int)Math.Pow(2, consecutiveFails) * 50);
            jitterMs += failurePenalty;
        }

        return TimeSpan.FromMilliseconds(jitterMs);
    }
}