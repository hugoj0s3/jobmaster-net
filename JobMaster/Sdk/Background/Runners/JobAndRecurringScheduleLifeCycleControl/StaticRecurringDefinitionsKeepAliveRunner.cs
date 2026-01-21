using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Background.Runners.JobAndRecurringScheduleLifeCycleControl;

internal class StaticRecurringDefinitionsKeepAliveRunner : JobMasterRunner
{
    private readonly IMasterRecurringSchedulesService masterRecurringSchedulesService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly JobMasterLockKeys lockKeys;

    public override TimeSpan SucceedInterval => TimeSpan.FromMinutes(1);

    public StaticRecurringDefinitionsKeepAliveRunner(IJobMasterBackgroundAgentWorker backgroundAgentWorker) : base(backgroundAgentWorker, bucketAwareLifeCycle: false, useSemaphore: true)
    {
        masterRecurringSchedulesService = backgroundAgentWorker.GetClusterAwareService<IMasterRecurringSchedulesService>();
        masterDistributedLockerService = backgroundAgentWorker.GetClusterAwareService<IMasterDistributedLockerService>();
        lockKeys = new JobMasterLockKeys(this.BackgroundAgentWorker.ClusterConnConfig.ClusterId);
    }

    public override Task OnStartAsync(CancellationToken ct)
    {
        return Task.CompletedTask;
    }

    public override async Task<OnTickResult> OnTickAsync(CancellationToken ct)
    {
        var lockDuration = TimeSpan.FromMinutes(2.5);
        var lockKey = lockKeys.StaticDefinitionsKeepAliveLock();
        var lockToken = masterDistributedLockerService.TryLock(lockKey, lockDuration);
        if (lockToken == null)
        {
            return OnTickResult.Locked(TimeSpan.FromSeconds(10));
        }

        try
        {
            var clusterId = BackgroundAgentWorker.ClusterConnConfig.ClusterId;
            var ids = StaticRecurringDefinitionIdsKeeper.GetAll(clusterId);
            if (ids.Count > 0)
            {
                masterRecurringSchedulesService.BulkUpdateStaticDefinitionLastEnsured(ids.ToList(), DateTime.UtcNow);
            }

            var cutoff = DateTime.UtcNow - TimeSpan.FromHours(1);
            await masterRecurringSchedulesService.InactivateStaticDefinitionsOlderThanAsync(cutoff);

            return OnTickResult.Success(this);
        }
        catch (Exception)
        {
            return OnTickResult.Failed(this);
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKey, lockToken);
        }
    }
}
