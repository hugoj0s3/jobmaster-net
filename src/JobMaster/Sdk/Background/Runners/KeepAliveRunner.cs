using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Maintains worker heartbeat signals to indicate the worker is alive and responsive.
/// This runner is essential for worker health monitoring and cluster coordination.
/// </summary>
/// <remarks>
/// <para>
/// The KeepAliveRunner performs the following critical functions:
/// </para>
/// <list type="bullet">
/// <item><description>Sends periodic heartbeat signals to the master heartbeat service</description></item>
/// <item><description>Maintains worker presence in the cluster for health monitoring</description></item>
/// <item><description>Enables other components to detect worker failures and timeouts</description></item>
/// <item><description>Provides low-latency health status updates with 100ms intervals</description></item>
/// </list>
/// <para>
/// The runner uses a very short 100ms interval to ensure responsive health monitoring,
/// allowing the system to quickly detect worker failures and trigger appropriate
/// recovery mechanisms such as bucket reassignment and job redistribution.
/// </para>
/// <para>
/// This runner operates without semaphore restrictions and uses shared lifecycle
/// management, making it lightweight and ensuring it doesn't interfere with
/// other critical system operations while maintaining continuous health signals.
/// </para>
/// </remarks>
internal class KeepAliveRunner : JobMasterRunner
{
    private IMasterHeartbeatService masterHeartbeatService;
    private readonly TimeSpan interval = TimeSpan.FromSeconds(5);
    
    public override TimeSpan SucceedInterval => interval;
    
    public KeepAliveRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: false)
    {
        masterHeartbeatService = backgroundAgentWorker.GetClusterAwareService<IMasterHeartbeatService>();
    }

    /// <summary>
    /// Executes one heartbeat cycle, sending a health signal to the master heartbeat service.
    /// </summary>
    /// <param name="ct">Cancellation token to handle graceful shutdown</param>
    /// <returns>A task representing the asynchronous heartbeat operation</returns>
    /// <remarks>
    /// <para>
    /// The heartbeat process is intentionally simple and lightweight:
    /// </para>
    /// <list type="number">
    /// <item><description>Delay for 100ms to maintain consistent heartbeat frequency</description></item>
    /// <item><description>Send heartbeat signal with the current worker's agent ID</description></item>
    /// </list>
    /// <para>
    /// The 100ms interval ensures that worker health status is updated frequently enough
    /// for other system components to detect failures quickly, while being lightweight
    /// enough to not impact system performance.
    /// </para>
    /// </remarks>
    public override Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        try
        {
            masterHeartbeatService.Heartbeat(BackgroundAgentWorker.AgentWorkerId);
            return Task.FromResult(OnTickResult.Success(this));
        }
        catch (Exception e)
        {
            return Task.FromResult(OnTickResult.Failed(this, e, "Heartbeat failed"));
        }
    }
}