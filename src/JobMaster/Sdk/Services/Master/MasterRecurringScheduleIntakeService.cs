using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Services.Master;

internal class MasterRecurringScheduleIntakeService : JobMasterClusterAwareComponent, IMasterRecurringScheduleIntakeService
{
    private readonly IMasterRecurringSchedulesRepository masterRecurringSchedulesRepository;
    private readonly IMasterClusterConfigurationService masterClusterConfigurationService;

    public MasterRecurringScheduleIntakeService(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IMasterRecurringSchedulesRepository masterRecurringSchedulesRepository,
        IMasterClusterConfigurationService masterClusterConfigurationService) : base(clusterConnectionConfig)
    {
        this.masterRecurringSchedulesRepository = masterRecurringSchedulesRepository;
        this.masterClusterConfigurationService = masterClusterConfigurationService;
    }

    public async Task BulkInsertIfNotExistsAsync(IList<RecurringScheduleRawModel> schedules)
    {
        var clusterMode = masterClusterConfigurationService.Get()?.ClusterMode;

        switch (clusterMode)
        {
            case ClusterMode.Archived:
            {
                var nonFinal = schedules.Where(s => !s.Status.IsFinalStatus()).ToList();
                if (nonFinal.Count > 0)
                {
                    throw new InvalidOperationException(
                        $"Cannot archive {nonFinal.Count} recurring schedule(s) not in a final status " +
                        $"(e.g. schedule '{nonFinal[0].Id}' has status '{nonFinal[0].Status}'). " +
                        "Only finalized recurring schedules can be archived.");
                }
                break;
            }
            case ClusterMode.Active:
            {
                var notActive = schedules.Where(s => s.Status != RecurringScheduleStatus.Active).ToList();
                if (notActive.Count > 0)
                {
                    throw new InvalidOperationException(
                        $"Cannot migrate {notActive.Count} recurring schedule(s) not in Active status " +
                        $"(e.g. schedule '{notActive[0].Id}' has status '{notActive[0].Status}'). " +
                        "Only Active recurring schedules can be migrated.");
                }
                break;
            }
            default:
                throw new InvalidOperationException(
                    $"Cluster '{this.ClusterConnConfig.ClusterId}' cannot receive recurring schedules from " +
                    $"another cluster while in ClusterMode '{clusterMode}'. Only Archived and Active clusters can.");
        }

        foreach (var schedule in schedules)
        {
            schedule.ReassignToCluster(this.ClusterConnConfig.ClusterId);
        }

        await masterRecurringSchedulesRepository.BulkInsertIfNotExistsAsync(schedules);
    }
}
