using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Jobs;

namespace JobMaster.Abstractions.Models;

/// <summary>
/// Returned by every one-off scheduling call: the scheduled job and where it was stored when the call returned.
/// The receipt doesn't change afterwards: <see cref="Status"/> keeps the value from scheduling time.
/// </summary>
public sealed class JobScheduleReceipt
{
    internal readonly JobRawModel model;
    private JobContext? context;

    internal JobScheduleReceipt(
        ScheduleReceiptStatus status, 
        JobRawModel model)
    {
        JobId = model.Id;
        Status = status;
        BucketId = model.BucketId;
        AgentWorkerId = model.AgentWorkerId;
        ClusterId = model.ClusterId;
        AgentConnectionId = model.AgentConnectionId?.IdValue;
        this.model = model;
    }

    /// <summary>Id of the scheduled job.</summary>
    public Guid JobId { get; }

    /// <summary>Where the job was stored when the scheduling call returned.</summary>
    public ScheduleReceiptStatus Status { get; }

    /// <summary>Cluster the job was scheduled on (the resolved one, not necessarily the one passed to the call).</summary>
    public string ClusterId { get; }

    /// <summary>Bucket the job was handed to. <c>null</c> when <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.</summary>
    public string? BucketId { get; }

    /// <summary>Agent worker that owns <see cref="BucketId"/>. <c>null</c> when <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.</summary>
    public string? AgentWorkerId { get; }
    
    /// <summary>
    /// Agent connection the job was saved on when <see cref="ScheduleReceiptStatus.AcceptedOnTransport"/>.
    /// <c>null</c> when <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.
    /// </summary>
    public string? AgentConnectionId { get; }

    /// <summary>The scheduled job.</summary>
    public JobContext Context => context ??= JobConvertUtil.ToJobContext(model);

    /// <summary>
    /// Makes sure the job is saved on master, saving it now if it isn't there yet. When it returns, the job is on
    /// master. Returns right away when <see cref="Status"/> is <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.
    /// <para>
    /// Rarely needed: if your agent connections are healthy, you don't need to call it at all. A confirmed job can
    /// start later than it would otherwise, because it's picked up from master instead of going straight to its
    /// worker. To confirm many jobs, use
    /// <see cref="IJobMasterSchedulerAdvanced.ConfirmOnMasterAsync(IEnumerable{JobScheduleReceipt})"/>.
    /// </para>
    /// <para>
    /// If it throws, the job may or may not be saved; calling it again is safe and never creates a second copy.
    /// JobMaster runs jobs at least once: confirming after the cluster's data retention has removed an
    /// already-finished job saves it again, and it runs again. Be careful calling it when the data retention is short:
    /// the shorter it is, the more likely a late confirm runs the job twice.
    /// </para>
    /// </summary>
    public Task ConfirmOnMasterAsync()
    {
        if (Status == ScheduleReceiptStatus.StoredOnMaster)
        {
            return Task.CompletedTask;
        }

        return JobMasterScheduler.Instance.Advanced.ConfirmOnMasterAsync(new[] { this });
    }

    /// <summary>
    /// Cancels the job on the cluster it was scheduled on. Works right after scheduling too, whatever the
    /// <see cref="Status"/>.
    /// <para>
    /// Returns <c>true</c> when the job is cancelled and won't run. Returns <c>false</c> when it can't be cancelled
    /// anymore: it's running or finished, or it's due to start in less than about a minute and a worker may
    /// already have it.
    /// </para>
    /// <para>
    /// If it throws, the job may or may not be cancelled; calling it again is safe.
    /// Same as <see cref="IJobMasterScheduler.CancelJobAsync(JobScheduleReceipt)"/>.
    /// </para>
    /// </summary>
    public async Task<bool> CancelAsync()
    {
        return await JobMasterScheduler.Instance.CancelJobAsync(this);
    }
}
