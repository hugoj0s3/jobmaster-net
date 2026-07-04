using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Removes agent connection records that have not sent a heartbeat within the dead-connection
/// threshold. <c>ProtectConnectionChanges</c> is about detecting a silently-changed underlying
/// connection at bootstrap (see <see cref="JobMaster.Sdk.Background.JobMasterRuntime"/>'s fingerprint
/// check) — it has nothing to do with deletion timing, so protected and unprotected connections
/// share the same dead threshold here. Deletion still only happens once the connection has no
/// buckets left (see <see cref="IMasterAgentConnectionService.SafeDeleteConnectionAsync"/>), so a
/// connection is never removed out from under buckets that still need to drain. A dead protected
/// connection also emits a critical warning as soon as it's detected, since it's expected to be
/// more consequential than an unprotected one going down.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class CleanupDeadAgentConnectionsRunner : JobMasterRunner
{
    private static readonly TimeSpan DeleteAgentConnectionThreshold = TimeSpan.FromMinutes(30);

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
            var nowUtc = DateTime.UtcNow;

            var connectionsToDelete = allConnections
                // Reserved connections (standalone, fallback-bucket) are expected to sit dead for long
                // stretches — the standalone one whenever no standalone worker is running, the fallback
                // one whenever no fallback bucket is active. Never delete either.
                .Where(c => c.Id.Name != JobMasterConstants.StandaloneAgentConnName &&
                            c.Id.Name != JobMasterConstants.MasterFallbackAgentConnName)
                .Where(c => c.LastHeartbeatAt < nowUtc - DeleteAgentConnectionThreshold)
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
