using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Services;

internal interface IJobMasterSchedulerClusterAware : IJobMasterClusterAwareService
{
    // Schedule*: the bucket it was handed to on the agent transport, or null when it was stored on master.
    BucketModel? Schedule(RecurringScheduleRawModel rawModel);
    BucketModel? Schedule(JobRawModel job);

    Task<BucketModel?> ScheduleAsync(RecurringScheduleRawModel rawModel);
    Task<BucketModel?> ScheduleAsync(JobRawModel job);
    Task BulkScheduleAsync(List<JobRawModel> jobs);

    // ConfirmOnMaster: saves each model on master (held on master / active) if it isn't there yet.
    Task ConfirmJobsOnMasterAsync(IList<JobRawModel> jobs);
    Task ConfirmRecurringSchedulesOnMasterAsync(IList<RecurringScheduleRawModel> schedules);
    // Receipt cancel: saves the model on master already cancelled if it isn't there yet. false = already on master.
    Task<bool> InsertCancelledJobAsync(JobRawModel job);
    Task<bool> InsertCancelledRecurringScheduleAsync(RecurringScheduleRawModel schedule);
    Task<bool> CancelJobAsync(Guid jobId);
    bool CancelJob(Guid jobId);
    Task<bool> CancelRecurringAsync(Guid recurringScheduleId);
    bool CancelRecurring(Guid recurringScheduleId);
    Task<bool> RescheduleAsync(Guid jobId, DateTime scheduledAt);
    bool Reschedule(Guid jobId, DateTime scheduledAt);
}