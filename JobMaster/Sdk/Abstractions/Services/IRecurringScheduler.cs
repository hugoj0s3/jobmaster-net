using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IRecurringSchedulePlanner : IJobMasterClusterAwareService
{
    Task ScheduleNextJobsAsync(RecurringScheduleRawModel schedule);
}