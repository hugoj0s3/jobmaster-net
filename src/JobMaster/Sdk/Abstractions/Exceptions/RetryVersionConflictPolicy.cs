using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Exceptions;

/// <summary>
/// Retries a read-check-write delegate when its versioned write throws <see cref="JobMasterVersionConflictException"/>
/// (another writer updated the item after it was read). Same shape as <see cref="RetryDeadlockPolicy"/>.
/// </summary>
/// <remarks>
/// <para>
/// The delegate must be the whole read-check-write, not just the write: each attempt reads the item again and applies
/// its rule to the current state, so a conflict caused by another process finishing, cancelling or moving the item
/// ends in that rule's answer (e.g. "not cancellable") instead of an overwrite.
/// </para>
/// <para>
/// Backoff and the sync <see cref="Thread.Sleep(TimeSpan)"/> path work as in <see cref="RetryDeadlockPolicy"/>:
/// constant interval plus up to 50% jitter, so two writers that collided don't retry in lockstep.
/// </para>
/// </remarks>
internal class RetryVersionConflictPolicy
{
    private readonly TimeSpan retryInterval;
    private readonly int maxRetryCount;

    /// <param name="retryInterval">Base wait between attempts; the actual wait is
    /// <c>retryInterval + Random(0, retryInterval/2)</c>.</param>
    /// <param name="maxRetryCount">Maximum number of retries after the first attempt: 3 means up to 4 runs of the
    /// delegate.</param>
    public RetryVersionConflictPolicy(TimeSpan retryInterval, int maxRetryCount)
    {
        this.retryInterval = retryInterval;
        this.maxRetryCount = maxRetryCount;
    }

    /// <summary>
    /// Runs <paramref name="func"/>, running it again on a version conflict. Rethrows the conflict once the retries are
    /// used up, and any other exception at once.
    /// </summary>
    public async Task<T> ExecAsync<T>(Func<Task<T>> func)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                return await func();
            }
            catch (JobMasterVersionConflictException) when (retryCount < maxRetryCount)
            {
                await Task.Delay(JitteredDelay());
                retryCount++;
            }
        }
    }

    /// <summary>Sync <see cref="ExecAsync{T}"/>; prefer the async one from async call paths.</summary>
    public T Exec<T>(Func<T> func)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                return func();
            }
            catch (JobMasterVersionConflictException) when (retryCount < maxRetryCount)
            {
                Thread.Sleep(JitteredDelay());
                retryCount++;
            }
        }
    }

    private TimeSpan JitteredDelay()
    {
        var baseMs = (int)retryInterval.TotalMilliseconds;
        var jitterMs = JobMasterRandomUtil.GetInt(0, baseMs / 2 + 1);
        return TimeSpan.FromMilliseconds(baseMs + jitterMs);
    }
}
