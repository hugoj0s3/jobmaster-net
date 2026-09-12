using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions;

internal class OperationThrottler
{
    private const int DefaultAcquireTimeoutMs = 10000;

    public int? Capacity { get; }
    private readonly SemaphoreSlim? semaphore;

    public int AcquireTimeoutMs { get; }

    public OperationThrottler(int? capacity, int acquireTimeoutMs = DefaultAcquireTimeoutMs)
    {
        Capacity = capacity;
        AcquireTimeoutMs = acquireTimeoutMs;
        if (capacity.HasValue && capacity.Value > 0)
        {
            semaphore = new SemaphoreSlim(capacity.Value, capacity.Value);
        }
    }

    // A failed Wait() falls through and runs func() anyway (fail-open, not fail-closed) -- see the
    // other overloads' own comments. When many callers are contending for the same scarce capacity,
    // they tend to hit that timeout in a tight cluster and would otherwise all fall through and fire
    // at once -- a synchronized burst against the very thing the throttler exists to pace. A small
    // random jitter before falling through spreads that cluster out instead.
    private static int JitterMs() => JobMasterRandomUtil.GetInt(2, 11) * 25; // 50-250ms, in 25ms steps

    public T Exec<T>(Func<T> func)
    {
        if (semaphore == null) return func();

        var acquired = semaphore.Wait(AcquireTimeoutMs);
        if (!acquired) Thread.Sleep(JitterMs());
        try
        {
            return func();
        }
        finally
        {
            if (acquired) semaphore.Release();
        }
    }

    public void Exec(Action func)
    {
        if (semaphore == null) { func(); return; }

        var acquired = semaphore.Wait(AcquireTimeoutMs);
        if (!acquired) Thread.Sleep(JitterMs());
        try
        {
            func();
        }
        finally
        {
            if (acquired) semaphore.Release();
        }
    }

    public async Task<T> ExecAsync<T>(Func<Task<T>> func)
    {
        if (semaphore == null) return await func();

        var acquired = await semaphore.WaitAsync(AcquireTimeoutMs);
        if (!acquired) await Task.Delay(JitterMs());
        try
        {
            return await func();
        }
        finally
        {
            if (acquired) semaphore.Release();
        }
    }

    public async Task ExecAsync(Func<Task> func)
    {
        if (semaphore == null) { await func(); return; }

        var acquired = await semaphore.WaitAsync(AcquireTimeoutMs);
        if (!acquired) await Task.Delay(JitterMs());
        try
        {
            await func();
        }
        finally
        {
            if (acquired) semaphore.Release();
        }
    }
}