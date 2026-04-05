namespace JobMaster.Sdk.Abstractions;

internal class OperationLimiter
{
    public int? Capacity { get; }
    private readonly SemaphoreSlim? semaphore;

    public OperationLimiter(int? capacity)
    {
        Capacity = capacity;

        if (capacity.HasValue && capacity.Value > 0)
        {
            semaphore = new SemaphoreSlim(capacity.Value, capacity.Value);
        }
    }
    
    public T Exec<T>(Func<T> func)
    {
        if (semaphore == null) return func();

        semaphore.Wait();

        try
        {
            return func();
        }
        finally
        {
            semaphore.Release();
        }
    }

    public void Exec(Action func)
    {
        if (semaphore == null) { func(); return; }

        semaphore.Wait();

        try
        {
            func();
        }
        finally
        {
            semaphore.Release();
        }
    }

    public async Task<T> ExecAsync<T>(Func<Task<T>> func)
    {
        if (semaphore == null) return await func();

        await semaphore.WaitAsync();

        try
        {
            return await func();
        }
        finally
        {
            semaphore.Release();
        }
    }

    public async Task ExecAsync(Func<Task> func)
    {
        if (semaphore == null)
        {
            await func();
            return;
        }

        await semaphore.WaitAsync();

        try
        {
            await func();
        }
        finally
        {
            semaphore.Release();
        }
    }

    public async Task ExecValueTaskAsync(Func<ValueTask> func)
    {
        if (semaphore == null)
        {
            await func();
            return;
        }

        await semaphore.WaitAsync();

        try
        {
            await func();
        }
        finally
        {
            semaphore.Release();
        }
    }
}
