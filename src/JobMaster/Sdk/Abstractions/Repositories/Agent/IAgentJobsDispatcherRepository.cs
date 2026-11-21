using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

internal interface IAgentJobsDispatcherRepository : IJobMasterClusterAwareComponent
{
    string PushSavePendingRecur(RecurringScheduleRawModel recurringScheduleRaw);
    Task<string> PushSavePendingRecurAsync(RecurringScheduleRawModel recurringScheduleRaw);
    
    string PushSavePendingJob(JobRawModel jobRaw);
    Task<string> PushSavePendingJobAsync(JobRawModel jobRaw);
    
    Task<IList<string>> BulkPushSavePendingJobAsync(string bucketId, IList<JobRawModel> jobRaw);
    
    string PushForProcessing(JobRawModel jobRaw);
    Task<string> PushForProcessingAsync(JobRawModel jobRaw);

    Task<IList<string>> BulkPushForProcessingAsync(string bucketId, IList<JobRawModel> jobRaw);

    Task<IList<JobRawModel>> PullForProcessingAsync(string bucketId, int numberOfJobs, DateTime? scheduleTo);
    
    /// <summary>
    /// Lease + ack, the usual at-least-once read of a queue (an SQS visibility timeout, a Service Bus peek-lock): a
    /// leased message stays stored, hidden from other leases for <paramref name="lease"/>. The caller saves the job on
    /// master, then acks its <c>AckKey</c> (<see cref="AckSavePendingJobsAsync"/>), which removes the message. One not
    /// acked by the end of its lease (save failed, worker stopped or killed) is leased again, so a job can be saved
    /// twice: saving must be idempotent (insert-if-not-exists).
    /// A message leased <see cref="JobMasterConstants.MaxSavePendingLeaseCount"/> times is not returned: it is moved
    /// to the bucket's terminated address and logged. Same for <see cref="LeaseSavePendingRecurAsync"/>.
    /// </summary>
    Task<IList<(JobRawModel Job, string AckKey)>> LeaseSavePendingJobsAsync(string bucketId, int numberOfJobs, TimeSpan lease);

    /// <summary>
    /// Confirms leased messages are done, removing them: call it only once their job is on master. The second half of
    /// <see cref="LeaseSavePendingJobsAsync"/>. Keys already gone (acked before) are ignored.
    /// </summary>
    Task AckSavePendingJobsAsync(string bucketId, IList<string> ackKeys);

    /// <summary>
    /// Lease + ack for recurring schedules: same as <see cref="LeaseSavePendingJobsAsync"/>, acked with
    /// <see cref="AckSavePendingRecurAsync"/> once the schedule is on master.
    /// </summary>
    Task<IList<(RecurringScheduleRawModel Schedule, string AckKey)>> LeaseSavePendingRecurAsync(string bucketId, int numberOfSchedules, TimeSpan lease);

    /// <summary>
    /// Confirms leased recurring schedule messages are done, removing them: call it only once their schedule is on
    /// master. The second half of <see cref="LeaseSavePendingRecurAsync"/>. Keys already gone are ignored.
    /// </summary>
    Task AckSavePendingRecurAsync(string bucketId, IList<string> ackKeys);
    Task<bool> HasJobsAsync(string bucketId);
    
    Task CreateBucketAsync(string bucketId);
    Task DestroyBucketAsync(string bucketId);
    
    void Initialize(JobMasterAgentConnectionConfig agentConnConfig);
    
    string AgentRepoTypeId { get; }
    
    bool IsPollingBasedForSaving { get; }
    bool IsPollingBasedForProcessing { get; }
}
