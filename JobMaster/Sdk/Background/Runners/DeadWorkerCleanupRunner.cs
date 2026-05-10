using System.Collections.Concurrent;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Periodically scans all registered agent workers and removes records for those that are
/// dead (<see cref="JobMaster.Sdk.Abstractions.Models.Agents.AgentWorkerModel.IsAlive"/> = false)
/// once their grace window has closed and their last heartbeat is older than the combined
/// heartbeat-threshold + cleanup-grace-period.
/// The current worker is always skipped; workers still within their
/// <see cref="JobMaster.Sdk.Abstractions.Models.Agents.AgentWorkerModel.StopGracePeriod"/> are left untouched.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
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
                
                var maxTimeToLive = JobMasterConstants.AgentHeartbeatThreshold.Add(JobMasterConstants.DeadWorkerCleanupGracePeriod);
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
