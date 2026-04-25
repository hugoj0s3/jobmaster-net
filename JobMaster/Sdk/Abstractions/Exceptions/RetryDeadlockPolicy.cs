using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Exceptions;

internal class RetryDeadlockPolicy
{
    private readonly IKnownExceptionIdentifier knownExceptionIdentifier;
    private readonly TimeSpan retryInterval;
    private readonly int maxRetryCount;

    public RetryDeadlockPolicy(
        IKnownExceptionIdentifier knownExceptionIdentifier,
        TimeSpan retryInterval,
        int maxRetryCount)
    {
        this.knownExceptionIdentifier = knownExceptionIdentifier;
        this.retryInterval = retryInterval;
        this.maxRetryCount = maxRetryCount;
    }

    public async Task<T> ExecAsync<T>(Func<Task<T>> func)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                return await func();
            }
            catch (Exception ex) when (ShouldRetry(ex, retryCount))
            {
                await Task.Delay(JitteredDelay());
                retryCount++;
            }
        }
    }

    public T Exec<T>(Func<T> func)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                return func();
            }
            catch (Exception ex) when (ShouldRetry(ex, retryCount))
            {
                Thread.Sleep(JitteredDelay());
                retryCount++;
            }
        }
    }

    public void Exec(Action action) => Exec(() =>
    {
        action();
        return true;
    });

    public Task ExecAsync(Func<Task> func) => ExecAsync(async () =>
    {
        await func();
        return true;
    });

    private bool ShouldRetry(Exception exception, int retryCount)
    {
        if (knownExceptionIdentifier.Identify(exception) == JobMasterKnownExceptionId.Deadlock
            && retryCount < maxRetryCount)
        {
            return true;
        }

        return false;
    }

    private TimeSpan JitteredDelay()
    {
        // Add up to 50% jitter on top of the base interval to avoid thundering herd
        // when multiple workers are deadlock victims of the same write and would
        // otherwise back off in lockstep.
        var baseMs = (int)retryInterval.TotalMilliseconds;
        var jitterMs = JobMasterRandomUtil.GetInt(0, baseMs / 2 + 1);
        return TimeSpan.FromMilliseconds(baseMs + jitterMs);
    }
}
