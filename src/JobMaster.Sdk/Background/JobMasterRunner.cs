using JobMaster.Sdk.Background.Runners;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Services.Master;
using System.Diagnostics;

namespace JobMaster.Sdk.Background;

/// <summary>
/// Abstract base class for all JobMaster background runners, providing common lifecycle management,
/// concurrency control, and execution infrastructure for distributed job processing components.
/// </summary>
/// <remarks>
/// <para>
/// The JobMasterRunnerBase provides essential infrastructure for all background processing runners:
/// </para>
/// <list type="bullet">
/// <item><description>Lifecycle management with independent or shared cancellation token support</description></item>
/// <item><description>Semaphore-based concurrency control to prevent resource exhaustion</description></item>
/// <item><description>Standardized error handling and recovery mechanisms</description></item>
/// <item><description>Jitter-based timing to prevent thundering herd effects</description></item>
/// <item><description>Graceful shutdown coordination with the background agent worker</description></item>
/// </list>
/// <para>
/// Lifecycle management modes:
/// - Independent lifecycle: Runners get their own cancellation token for fine-grained control
/// - Shared lifecycle: Runners share the main worker token for coordinated shutdown
/// </para>
/// <para>
/// Concurrency control:
/// - Semaphore usage can be enabled/disabled based on runner requirements
/// - Prevents system overload by limiting concurrent runner execution
/// - Automatic semaphore acquisition and release with proper exception handling
/// </para>
/// <para>
/// All concrete runner implementations must implement OnTickAsync() to define their
/// specific processing logic, while optionally overriding lifecycle hooks for
/// custom initialization, cleanup, and error handling behavior.
/// </para>
/// </remarks>
public abstract class JobMasterRunner : IAsyncDisposable, IJobMasterRunner
{
    private CancellationTokenSource? cts;
    private Task? taskRunner;
    private const int MaxOfConsecutiveFails = 3;

    protected IJobMasterBackgroundAgentWorker BackgroundAgentWorker;
    private readonly bool useSemaphore;
    private readonly bool bucketAwareLifeCycle;
    protected readonly IJobMasterLogger logger;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="backgroundAgentWorker"></param>
    /// <param name="bucketAwareLifeCycle">If true doesn't stop the main worker when stop. It has independent lifecycle, but still respect main worker shutdown</param>
    /// <param name="useSemaphore"></param>
    protected JobMasterRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker, bool bucketAwareLifeCycle, bool useSemaphore = true)
    {
        this.BackgroundAgentWorker = backgroundAgentWorker;
        this.useSemaphore = useSemaphore;
        this.bucketAwareLifeCycle = bucketAwareLifeCycle;
        this.logger = backgroundAgentWorker.GetClusterAwareService<IJobMasterLogger>();
    }

    /// <summary>
    /// Starts the runner by initializing the cancellation token and beginning the execution loop.
    /// </summary>
    /// <returns>A task representing the asynchronous start operation</returns>
    /// <remarks>
    /// <para>
    /// The start process configures the runner based on its lifecycle mode:
    /// </para>
    /// <list type="bullet">
    /// <item><description>Independent lifecycle: Creates a new cancellation token source for fine-grained control</description></item>
    /// <item><description>Shared lifecycle: Uses the main worker's cancellation token for coordinated shutdown</description></item>
    /// </list>
    /// <para>
    /// The runner execution loop is started as a background task that will continue
    /// until cancellation is requested or an unhandled exception occurs.
    /// </para>
    /// </remarks>
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

    /// <summary>
    /// Stops the runner by cancelling its execution and removing it from the worker's runner collection.
    /// </summary>
    /// <returns>A task representing the asynchronous stop operation</returns>
    /// <remarks>
    /// <para>
    /// The stop process performs the following cleanup operations:
    /// </para>
    /// <list type="number">
    /// <item><description>Removes the runner from the background agent worker's collection</description></item>
    /// <item><description>Cancels the runner's cancellation token to signal shutdown</description></item>
    /// <item><description>Waits for the runner task to complete gracefully</description></item>
    /// <item><description>Handles any exceptions during shutdown to prevent blocking</description></item>
    /// </list>
    /// <para>
    /// This method ensures proper cleanup and resource disposal while preventing
    /// shutdown operations from hanging due to unhandled exceptions in runner tasks.
    /// </para>
    /// </remarks>
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

    /// <summary>
    /// Main execution loop for the runner, handling lifecycle events, concurrency control, and error management.
    /// </summary>
    /// <param name="ct">Cancellation token to handle graceful shutdown</param>
    /// <returns>A task representing the asynchronous execution loop</returns>
    /// <remarks>
    /// <para>
    /// The execution loop performs the following operations:
    /// </para>
    /// <list type="number">
    /// <item><description>Calls OnStartAsync() for runner initialization</description></item>
    /// <item><description>Enters main processing loop while not cancelled</description></item>
    /// <item><description>Acquires semaphore if semaphore usage is enabled</description></item>
    /// <item><description>Executes OnTickAsync() with comprehensive error handling</description></item>
    /// <item><description>Applies jitter delay to prevent thundering herd effects</description></item>
    /// <item><description>Releases semaphore if acquired</description></item>
    /// <item><description>Calls OnStopAsync() for cleanup when loop exits</description></item>
    /// </list>
    /// <para>
    /// Error handling strategy:
    /// - OperationCanceledException is caught and handled gracefully during shutdown
    /// - Other exceptions trigger OnErrorAsync() before being re-thrown
    /// - Semaphore is properly released even in error scenarios
    /// </para>
    /// </remarks>
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

                logger.Error($"Runner {this.GetType().Name} failed {ConsecutiveFailedCount} times in a row. {ex.StackTrace}", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId);
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

    /// <summary>
    /// Handles errors that occur during runner execution. Override this method to implement custom error handling logic.
    /// </summary>
    /// <param name="ex">The exception that occurred during execution</param>
    /// <param name="ct">Cancellation token for the error handling operation</param>
    /// <returns>A task representing the asynchronous error handling operation</returns>
    /// <remarks>
    /// <para>
    /// The default implementation does nothing, allowing concrete runners to implement
    /// their own error handling strategies such as:
    /// </para>
    /// <list type="bullet">
    /// <item><description>Logging errors for monitoring and debugging</description></item>
    /// <item><description>Implementing retry logic for transient failures</description></item>
    /// <item><description>Sending alerts for critical system errors</description></item>
    /// <item><description>Graceful degradation or fallback mechanisms</description></item>
    /// </list>
    /// <para>
    /// Note that after OnErrorAsync() is called, the exception is re-thrown to maintain
    /// the standard exception propagation behavior, so this method should focus on
    /// logging, monitoring, and recovery preparation rather than exception suppression.
    /// </para>
    /// </remarks>
    public virtual Task OnErrorAsync(Exception ex, CancellationToken ct)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Called when the runner starts, before entering the main execution loop. Override this method to implement custom initialization logic.
    /// </summary>
    /// <param name="ct">Cancellation token for the start operation</param>
    /// <returns>A task representing the asynchronous start operation</returns>
    /// <remarks>
    /// <para>
    /// The default implementation does nothing, allowing concrete runners to implement
    /// their own initialization logic such as:
    /// </para>
    /// <list type="bullet">
    /// <item><description>Setting up connections to external services</description></item>
    /// <item><description>Initializing caches or data structures</description></item>
    /// <item><description>Performing health checks or validation</description></item>
    /// <item><description>Loading configuration or state information</description></item>
    /// </list>
    /// <para>
    /// This method is called once per runner lifecycle, before any OnTickAsync() calls,
    /// making it ideal for one-time setup operations that need to complete before
    /// the runner begins its regular processing cycles.
    /// </para>
    /// </remarks>
    public virtual Task OnStartAsync(CancellationToken ct)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Executes one processing cycle of the runner. This method must be implemented by concrete runner classes to define their specific processing logic.
    /// </summary>
    /// <param name="ct">Cancellation token to handle graceful shutdown</param>
    /// <returns>A task representing the asynchronous processing operation</returns>
    /// <remarks>
    /// <para>
    /// This is the core method that defines what the runner does during each execution cycle.
    /// Implementations should:
    /// </para>
    /// <list type="bullet">
    /// <item><description>Perform the runner's primary processing logic</description></item>
    /// <item><description>Handle cancellation tokens appropriately for graceful shutdown</description></item>
    /// <item><description>Include appropriate delays to control processing frequency</description></item>
    /// <item><description>Implement error handling for recoverable failures</description></item>
    /// <item><description>Respect system resource constraints and concurrency limits</description></item>
    /// </list>
    /// <para>
    /// This method is called repeatedly in a loop until the runner is stopped or cancelled.
    /// The base class handles semaphore acquisition/release and jitter delays automatically.
    /// </para>
    /// </remarks>
    public abstract Task<OnTickResult> OnTickAsync(CancellationToken ct);

    /// <summary>
    /// Called when the runner stops, after exiting the main execution loop. Override this method to implement custom cleanup logic.
    /// </summary>
    /// <returns>A task representing the asynchronous stop operation</returns>
    /// <remarks>
    /// <para>
    /// The default implementation does nothing, allowing concrete runners to implement
    /// their own cleanup logic such as:
    /// </para>
    /// <list type="bullet">
    /// <item><description>Closing connections to external services</description></item>
    /// <item><description>Flushing pending operations or data</description></item>
    /// <item><description>Releasing resources or locks</description></item>
    /// <item><description>Persisting final state information</description></item>
    /// </list>
    /// <para>
    /// This method is called once per runner lifecycle, after the main execution loop exits,
    /// making it ideal for cleanup operations that need to complete before the runner
    /// is fully stopped and disposed.
    /// </para>
    /// </remarks>
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