using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services;

internal interface IJobMasterSchedulerClusterAware : IJobMasterClusterAwareService
{
    void Schedule(RecurringScheduleRawModel rawModel);
    void Schedule(JobRawModel job);

    Task ScheduleAsync(RecurringScheduleRawModel rawModel);
    Task ScheduleAsync(JobRawModel job);
    Task BulkScheduleAsync(List<JobRawModel> jobs);
    Task<bool> CancelJobAsync(Guid jobId);
    bool CancelJob(Guid id);
    Task<bool> CancelRecurringAsync(Guid id);
    bool CancelRecurring(Guid id);
    Task<bool> ReScheduleAsync(Guid jobId, DateTime scheduledAt);
    bool ReSchedule(Guid jobId, DateTime scheduledAt);
}