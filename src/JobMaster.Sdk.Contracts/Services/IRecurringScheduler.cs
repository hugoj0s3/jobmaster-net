using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;

namespace JobMaster.Sdk.Contracts.Services;

public interface IRecurringSchedulePlanner : IJobMasterClusterAwareService
{
    Task ScheduleNextJobsAsync(RecurringScheduleRawModel schedule);
}