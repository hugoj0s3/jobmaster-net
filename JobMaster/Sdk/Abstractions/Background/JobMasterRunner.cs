using System.ComponentModel;
using System.Diagnostics;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Background.Runners;

namespace JobMaster.Sdk.Abstractions.Background;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class JobMasterRunner : IAsyncDisposable, IJobMasterRunner
{
    private CancellationTokenSource? cts;
    private Task? taskRunner;
    private const int MaxOfConsecutiveFails = 3;

    protected IJobMasterBackgroundAgentWorker BackgroundAgentWorker;
    private readonly bool useSemaphore;
    private readonly bool bucketAwareLifeCycle;
    protected readonly IJobMasterLogger logger;
    
    protected JobMasterRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker, bool bucketAwareLifeCycle, bool useSemaphore = true)
    {
        this.BackgroundAgentWorker = backgroundAgentWorker;
        this.useSemaphore = useSemaphore;
        this.bucketAwareLifeCycle = bucketAwareLifeCycle;
        this.logger = backgroundAgentWorker.GetClusterAwareService<IJobMasterLogger>();
    }
    
    public Task StartAsync()
    {
        if (bucketAwareLifeCycle)
        {
            // Independent runners get their own token but still respect main worker shutdown
            cts = new CancellationTokenSource();
        }
        else
        {
            // Critical infrastructure runners share the main worker token
            cts = BackgroundAgentWorker.CancellationTokenSource;
        }
        
        taskRunner = Task.Run(() => RunAsync(cts.Token), cts.Token);
        
        this.BackgroundAgentWorker.Runners.Add(this);
        return Task.CompletedTask;
    }
    
    public async Task StopAsync()
    {
        this.BackgroundAgentWorker.Runners.Remove(this);
        if (cts is null)
        {
            await OnStopAsync();
            return;
        }

        if (!cts.IsCancellationRequested)
        {
#if NETSTANDARD2_0
            cts.Cancel();
#else
            await cts.CancelAsync();
#endif
        }

        if (taskRunner is not null && 
            taskRunner.Status != TaskStatus.Faulted && 
            taskRunner.Status != TaskStatus.Canceled && 
            ConsecutiveFailedCount < MaxOfConsecutiveFails)
        {
            try
            {
                // Wait for runner to stop, but with a timeout to prevent hanging
                var timeout = Task.Delay(TimeSpan.FromSeconds(10));
                var completedTask = await Task.WhenAny(taskRunner, timeout);
                
                if (completedTask == timeout)
                {
                    // Runner didn't stop in time, likely not respecting cancellation token
                    logger.Warn($"Runner {this.GetType().Name} did not stop within 10 seconds. Forcing shutdown.", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                }
                else
                {
                    // Runner completed, check if it threw an exception
                    await taskRunner;
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or TaskCanceledException)
            {
                // Expected during shutdown, no action needed
            }
            // Let other exceptions bubble up to be handled by the caller
        }
        
        await OnStopAsync();
    }

    public IJobMasterRuntime? Runtime => BackgroundAgentWorker.Runtime;
    
    private async Task RunAsync(CancellationToken ct)
    {
        await OnStartAsync(ct);

        while (true)
        {
            if (ConsecutiveFailedCount >= MaxOfConsecutiveFails && this.GetType() != typeof(KeepAliveRunner))
            {
                break;
            }
            
            if (ct.IsCancellationRequested)
            {
                break;
            }
            
            if (BackgroundAgentWorker.CancellationTokenSource.IsCancellationRequested)
            {
                break;
            }
            
            // Skip execution during worker initialization (except KeepAliveRunner)
            // This prevents deadlocks during bucket creation
            if (!BackgroundAgentWorker.IsInitialized && this.GetType() != typeof(KeepAliveRunner))
            {
                await RunnerDelayUtil.DelayAsync(TimeSpan.FromSeconds(5), ct);
                continue;
            }
            
            SemaphoreSlim? semaphoreSlimToRelease = null;
            
            if (useSemaphore)
            {
                semaphoreSlimToRelease = bucketAwareLifeCycle
                    ? BackgroundAgentWorker.BucketAwareSemaphoreSlim
                    : BackgroundAgentWorker.MainSemaphoreSlim;

                var hasAcquiredSemaphore = await semaphoreSlimToRelease.WaitAsync(TimeSpan.FromSeconds(30), ct);

                if (!hasAcquiredSemaphore)
                {
                    await RunnerDelayUtil.DelayAsync(TimeSpan.FromSeconds(1), ct);
                    continue;
                }
            }

            var plannedDelay = TimeSpan.Zero;
            var plannedEarlyReleaseChance = 0.0;

            try
            {
                var sw = Stopwatch.StartNew();
                var result = await OnTickAsync(ct);
                if (result.Status == TicketResultStatus.Failed)
                {
                    ConsecutiveFailedCount++;
                }
                else if (result.Status == TicketResultStatus.Success && ConsecutiveFailedCount > 0)
                {
                    ConsecutiveFailedCount = 0;
                }
                
                // Only apply delay if the operation was successful or didn't run
                // Failed operations will be retried after their specified delay
                (plannedDelay, plannedEarlyReleaseChance) =  CalcDelay(result);
                sw.Stop();
                logger.Debug($"Runner {this.GetType().Name} tick completed. status={result.Status} elapsedMs={sw.ElapsedMilliseconds} plannedDelayMs={(long)plannedDelay.TotalMilliseconds}", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                
                if (result.Status == TicketResultStatus.Failed)
                {
                    var ex = result.Exception ?? new Exception(result.ErrorMessage ?? "Unknown error");
                    await OnErrorAsync(ex, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // Measure failure tick duration for observability
                logger.Debug($"Runner {this.GetType().Name} tick failed quickly. elapsed unknown due to exception.", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
                ConsecutiveFailedCount++;
                await OnErrorAsync(ex, ct);

                plannedDelay = OnTickResult.Failed(this).Delay;
                plannedEarlyReleaseChance = 0.0;

                logger.Error($"Runner {this.GetType().Name} failed {ConsecutiveFailedCount} times in a row.", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId, exception: ex);
            }
            finally
            {
                TimeSpan timeSpanBeforeRelease = RunnerDelayUtil.MaxTimeToReleaseSemaphore;
                TimeSpan timeSpanAfterRelease = TimeSpan.Zero;
                if (plannedDelay < RunnerDelayUtil.MaxTimeToReleaseSemaphore)
                {
                    timeSpanBeforeRelease = plannedDelay;
                }
                else
                {
                    timeSpanAfterRelease = plannedDelay;
                }
                
                var firstDelayDuration = await RunnerDelayUtil.DelayAsync(timeSpanBeforeRelease, ct, earlyReleaseChance: plannedEarlyReleaseChance);
                semaphoreSlimToRelease?.Release();

                if (timeSpanAfterRelease > firstDelayDuration)
                {
                    await RunnerDelayUtil.DelayAsync(timeSpanAfterRelease - firstDelayDuration, ct);
                }
                
                await RunnerDelayUtil.DelayAsync(RunnerDelayUtil.CalcJitter(ConsecutiveFailedCount), ct);
            }
        }

        if (!bucketAwareLifeCycle)
        {
            await BackgroundAgentWorker.StopImmediatelyAsync();
        }
        else
        {
            await StopAsync();
        }
    }

    private (TimeSpan, double) CalcDelay(OnTickResult onTickResult)
    {
        TimeSpan plannedDelay = TimeSpan.FromSeconds(1);
        double plannedEarlyReleaseChance = 0.1;
        switch (onTickResult.Status)
        {
            case TicketResultStatus.Success when (this.BackgroundAgentWorker.IsOnWarmUpTime()):
                plannedDelay = WarmUpInterval < onTickResult.Delay ? WarmUpInterval : onTickResult.Delay;
                plannedEarlyReleaseChance = 0.75;
                break;
            case TicketResultStatus.Success:
                plannedDelay = onTickResult.Delay;
                break;
            case TicketResultStatus.Skipped:
                plannedDelay = onTickResult.Delay;
                plannedEarlyReleaseChance = 0.9;
                break;
            case TicketResultStatus.Locked when this.BackgroundAgentWorker.IsOnWarmUpTime():
                plannedDelay = WarmUpInterval < onTickResult.Delay ? WarmUpInterval : onTickResult.Delay;
                plannedEarlyReleaseChance = 0.75;
                break;
            case TicketResultStatus.Locked:
                plannedDelay = onTickResult.Delay;
                plannedEarlyReleaseChance = 0.9;
                break;
            case TicketResultStatus.Failed:
                plannedDelay = onTickResult.Delay;
                break;
        }
        
        return (plannedDelay, plannedEarlyReleaseChance);
    }
    
    public virtual Task OnErrorAsync(Exception ex, CancellationToken ct)
    {
        return Task.CompletedTask;
    }
    
    public virtual Task OnStartAsync(CancellationToken ct)
    {
        return Task.CompletedTask;
    }
    
    public abstract Task<OnTickResult> OnTickAsync(CancellationToken ct);
    
    public virtual Task OnStopAsync()
    {
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(); 
        cts?.Dispose(); 
    }
    
    public abstract TimeSpan SucceedInterval { get; }
    public virtual TimeSpan WarmUpInterval => this.SucceedInterval;
    public int ConsecutiveFailedCount { get; private set; }
}