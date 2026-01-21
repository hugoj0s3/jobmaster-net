using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Abstractions;

[EditorBrowsable(EditorBrowsableState.Never)]
public class OperationThrottler
{
    private readonly IJobMasterLogger? logger;
    public int? Capacity { get; }
    private readonly SemaphoreSlim? semaphore;
    public TimeSpan Timeout { get; }

    public OperationThrottler(int? capacity, IJobMasterLogger? logger = null) 
        : this(capacity, TimeSpan.FromMilliseconds(1500), logger)
    {
    }

    public OperationThrottler(int? capacity, TimeSpan timeout, IJobMasterLogger? logger = null)
    {
        this.logger = logger;
        Capacity = capacity;
        Timeout = timeout;

        if (capacity.HasValue && capacity.Value > 0)
        {
            semaphore = new SemaphoreSlim(capacity.Value, capacity.Value);
        }
    }
    
    public T Exec<T>(Func<T> func)
    {
        if (semaphore == null) return func();

        var acquired = semaphore.Wait(this.Timeout);
        if (!acquired)
        {
            logger?.Debug($"OperationThrottler not acquired (sync<T>) in {Timeout.TotalMilliseconds}ms; capacity={Capacity}");
            return func();
        }

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

        var acquired = semaphore.Wait(this.Timeout);
        if (!acquired)
        {
            logger?.Debug($"OperationThrottler not acquired (sync) in {Timeout.TotalMilliseconds}ms; capacity={Capacity}");
            func();
            return;
        }

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

        var acquired = await semaphore.WaitAsync(Timeout);
        if (!acquired)
        {
            logger?.Debug($"OperationThrottler not acquired (async<T>) within {Timeout.TotalMilliseconds}ms; executing without throttle. capacity={Capacity}");
            return await func();
        }

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

        var acquired = await semaphore.WaitAsync(Timeout);
        if (!acquired)
        {
            logger?.Debug($"OperationThrottler not acquired (async) within {Timeout.TotalMilliseconds}ms; executing without throttle. capacity={Capacity}");
            await func();
            return;
        }

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

        var acquired = await semaphore.WaitAsync(Timeout);
        if (!acquired)
        {
            logger?.Debug($"OperationThrottler not acquired (valuetask) within {Timeout.TotalMilliseconds}ms; executing without throttle. capacity={Capacity}");
            await func();
            return;
        }

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