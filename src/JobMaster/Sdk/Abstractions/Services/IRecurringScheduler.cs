using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services;

public interface IRecurringSchedulePlanner : IJobMasterClusterAwareService
{
    Task ScheduleNextJobsAsync(RecurringScheduleRawModel schedule);
}