using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services;

internal interface IRecurringSchedulePlanner : IJobMasterClusterAwareService
{
    Task ScheduleNextJobsAsync(RecurringScheduleRawModel schedule);
}