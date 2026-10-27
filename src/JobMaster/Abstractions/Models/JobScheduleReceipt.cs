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

    internal JobScheduleReceipt(
        ScheduleReceiptStatus status, 
        JobRawModel model)
    {
        Status = status;
        BucketId = model.BucketId;
        AgentWorkerId = model.AgentWorkerId;
        ClusterId = model.ClusterId;
        AgentConnectionId = model.AgentConnectionId?.IdValue;
        this.model = model;
    }

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
    public JobContext GetJobContext() => JobConvertUtil.ToJobContext(model);

    /// <summary>
    /// Makes sure the job is saved on master, saving it now if it isn't there yet. When it returns, the job is on
    /// master. Returns right away when <see cref="Status"/> is <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.
    /// <para>
    /// Not recommended unless you need that guarantee: a confirmed job may start later than it would otherwise,
    /// because it's picked up from master instead of going straight to its worker. To confirm many jobs, use
    /// <see cref="IJobMasterSchedulerAdvanced.ConfirmOnMasterAsync(IEnumerable{JobScheduleReceipt})"/>, which also explains
    /// what happens on errors and when confirming late.
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
}
