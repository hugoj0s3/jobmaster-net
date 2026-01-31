using System.Collections.Concurrent;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Monitors and cleans up dead worker records from the system.
/// This runner identifies workers that are no longer alive and removes their records
/// after applying a grace period based on the agent's TransientThreshold configuration.
/// Before deletion, it signals an immediate stop via distributed locks to ensure graceful shutdown.
/// </summary>
/// <remarks>
/// <para><strong>Execution Interval:</strong> Every 5 minutes</para>
/// <para><strong>Lifecycle:</strong> Global runner (useIndependentLifecycle: false, useSemaphore: true)</para>
/// <para><strong>Key Operations:</strong></para>
/// <list type="bullet">
/// <item>Queries all workers and identifies dead ones (!IsAlive)</item>
/// <item>Applies grace period using agent-specific TransientThreshold</item>
/// <item>Sets WorkerImmediateStopLock before deletion for graceful shutdown</item>
/// <item>Deletes dead worker records via masterAgentsService.DeleteWorkerAsync()</item>
/// </list>
/// <para><strong>Safety Features:</strong></para>
/// <list type="bullet">
/// <item>Never deletes the current worker (self-protection)</item>
/// <item>Respects per-agent TransientThreshold for grace periods</item>
/// <item>Uses distributed locks for coordinated shutdown</item>
/// <item>Exception handling prevents affecting other operations</item>
/// </list>
/// </remarks>
internal class DeadWorkerCleanupRunner : JobMasterRunner
{
    private readonly IMasterAgentWorkersService masterAgentWorkersService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly JobMasterLockKeys lockKeys;
    
    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(5);
    
    public DeadWorkerCleanupRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) 
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterAgentWorkersService = backgroundAgentWorker.GetClusterAwareService<IMasterAgentWorkersService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(backgroundAgentWorker.ClusterConnConfig.ClusterId);
    }
    
    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        try
        {
            // Get all workers (including dead ones)
            var allWorkers = await masterAgentWorkersService.QueryWorkersAsync(useCache: false);
            
            // Find dead workers that need cleanup
            var deadWorkers = allWorkers.Where(w => !w.IsAlive).ToList();
            
            foreach (var deadWorker in deadWorkers)
            {
                // Skip if it's the current worker (shouldn't happen, but safety check)
                if (deadWorker.Id == BackgroundAgentWorker.AgentWorkerId)
                {
                    continue;
                }
                
                var stopDeadline = (deadWorker.StopRequestedAt ?? deadWorker.LastHeartbeat)
                    .Add(deadWorker.StopGracePeriod ?? JobMasterConstants.DefaultGracefulStopPeriod);
                if (DateTime.UtcNow.AddMinutes(5) < stopDeadline)
                {
                    continue;
                }
                
                var maxTimeToLive = JobMasterConstants.HeartbeatThreshold.Add(JobMasterConstants.DeadWorkerCleanupGracePeriod);
                if (deadWorker.LastHeartbeat > DateTime.UtcNow.Subtract(maxTimeToLive)) 
                {
                    continue;
                }
                
                masterDistributedLockerService
                    .TryLock(lockKeys.WorkerImmediateStopLock(deadWorker.Id), TimeSpan.FromHours(1));
                
                // Delete the dead worker record
                await masterAgentWorkersService.DeleteWorkerAsync(deadWorker.Id);
            }
            
            if (deadWorkers.Any())
            {
                // Log cleanup activity (you can add logging here if needed)
                // Console.WriteLine($"Cleaned up {deadWorkers.Count} dead workers");
            }
        }
        catch (Exception ex)
        {
            // Return failed status with exception details
            logger.Error($"Failed to clean up dead workers", JobMasterLogSubjectType.AgentWorker, BackgroundAgentWorker.AgentWorkerId, ex);
            return OnTickResult.Failed(this, ex, "Dead worker cleanup failed");
        }
        
        // Check for dead workers every 5 minutes
        return OnTickResult.Success(this);
    }
}
