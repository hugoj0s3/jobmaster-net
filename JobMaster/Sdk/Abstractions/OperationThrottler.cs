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

    public T Exec<T>(Func<T> func)
    {
        if (semaphore == null) return func();

        var acquired = semaphore.Wait(AcquireTimeoutMs);
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