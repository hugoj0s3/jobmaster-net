using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners;

/// <summary>
/// Removes host records that have not sent a heartbeat within the dead-host threshold.
/// Runs every <see cref="SucceedInterval"/>.
/// </summary>
internal class CleanupDeadHostsRunner : JobMasterRunner
{
    private static readonly TimeSpan DeadHostThreshold = TimeSpan.FromMinutes(5);
    
    private readonly IMasterHostService masterHostService;

    public CleanupDeadHostsRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker)
        : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: false)
    {
        masterHostService = backgroundAgentWorker.GetClusterAwareService<IMasterHostService>();
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        try
        {
            var allHosts = await masterHostService.QueryAllAsync();
            var deadHosts = allHosts.Where(h => h.LastHeartbeat < DateTime.UtcNow - DeadHostThreshold).ToList();
            
            if (deadHosts.Any())
            {
                var deadHostIds = deadHosts.Select(h => h.Id.IdValue).ToList();
                await masterHostService.DeleteHostsAsync(deadHostIds);
                
                logger.Info($"Deleted {deadHosts.Count} dead hosts: {string.Join(", ", deadHostIds)}");
            }
            
            return OnTickResult.Success(this);
        }
        catch (Exception e)
        {
            return OnTickResult.Failed(this, e, "Failed to cleanup dead hosts");
        }
    }
    
    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(5);
}
