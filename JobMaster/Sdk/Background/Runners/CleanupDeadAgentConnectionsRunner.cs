using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

internal class CleanupDeadAgentConnectionsRunner : JobMasterRunner
{
    private static readonly TimeSpan DeadAgentConnectionThreshold = TimeSpan.FromMinutes(10);
    
    private readonly IMasterAgentConnectionService masterAgentConnectionService;

    public CleanupDeadAgentConnectionsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: false)
    {
        masterAgentConnectionService = backgroundAgentWorker.GetClusterAwareService<IMasterAgentConnectionService>();
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        try
        {
            var allConnections = await masterAgentConnectionService.QueryAllAsync(useCache: false);
            
            var connectionsToDelete = allConnections
                .Where(c => (c.LastHeartbeatAt ?? c.FootprintCreatedAt) < DateTime.UtcNow - DeadAgentConnectionThreshold)
                .ToList();
            
            var deadProtectedConnections = allConnections
                .Where(c => !c.IsAlive() && c.ProtectConnectionChanges)
                .ToList();
            
            if (deadProtectedConnections.Any())
            {
                foreach (var connection in deadProtectedConnections)
                {
                    var hasBuckets = await masterAgentConnectionService.HasBucketsAsync(connection.Id);
                    if (hasBuckets)
                    {
                        logger.Critical($"Protected agent connection is dead and has buckets - jobs will be lost if connection is not restored: {connection.Id.IdValue}");
                    }
                }
            }
            
            if (connectionsToDelete.Any())
            {
                var successfulDeletes = new List<string>();
                var failedDeletes = new List<string>();
                
                foreach (var connection in connectionsToDelete)
                {
                    var deleted = await masterAgentConnectionService.SafeDeleteConnectionAsync(connection.Id);
                    if (deleted)
                    {
                        successfulDeletes.Add(connection.Id.IdValue);
                    }
                    else
                    {
                        failedDeletes.Add(connection.Id.IdValue);
                    }
                }
                
                if (successfulDeletes.Any())
                {
                    logger.Info($"Successfully deleted {successfulDeletes.Count} dead agent connections: {string.Join(", ", successfulDeletes)}");
                }
                
                if (failedDeletes.Any())
                {
                    logger.Warn($"Failed to delete {failedDeletes.Count} dead agent connections (has buckets): {string.Join(", ", failedDeletes)}");
                }
            }
            
            return OnTickResult.Success(this);
        }
        catch (Exception e)
        {
            return OnTickResult.Failed(this, e, "Failed to cleanup dead agent connections");
        }
    }
    
    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(5);
}
