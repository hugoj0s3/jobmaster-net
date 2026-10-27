using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.Abstractions.Models;

/// <summary>
/// Returned by every recurring scheduling call: the recurring schedule and where it was stored when the call
/// returned. The receipt doesn't change afterwards: <see cref="Status"/> keeps the value from scheduling time.
/// </summary>
public sealed class RecurringScheduleReceipt
{
    internal readonly RecurringScheduleRawModel model;

    internal RecurringScheduleReceipt(
        ScheduleReceiptStatus status,
        RecurringScheduleRawModel model)
    {
        Status = status;
        BucketId = model.BucketId;
        AgentWorkerId = model.AgentWorkerId;
        ClusterId = model.ClusterId;
        AgentConnectionId = model.AgentConnectionId?.IdValue;
        this.model = model;
    }

    /// <summary>Where the schedule was stored when the scheduling call returned.</summary>
    public ScheduleReceiptStatus Status { get; }

    /// <summary>Cluster the schedule was created on (the resolved one, not necessarily the one passed to the call).</summary>
    public string ClusterId { get; }

    /// <summary>Bucket the schedule was handed to. <c>null</c> when <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.</summary>
    public string? BucketId { get; }

    /// <summary>Agent worker that owns <see cref="BucketId"/>. <c>null</c> when <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.</summary>
    public string? AgentWorkerId { get; }

    /// <summary>
    /// Agent connection the schedule was saved on when <see cref="ScheduleReceiptStatus.AcceptedOnTransport"/>.
    /// <c>null</c> when <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.
    /// </summary>
    public string? AgentConnectionId { get; }

    /// <summary>The recurring schedule.</summary>
    public RecurringScheduleContext GetRecurringScheduleContext() => RecurringScheduleConvertUtil.ToContext(model);

    /// <summary>
    /// Makes sure the schedule is saved on master, saving it now if it isn't there yet. When it returns, the schedule
    /// is on master. Returns right away when <see cref="Status"/> is <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.
    /// Not recommended unless you need that guarantee; see
    /// <see cref="IJobMasterSchedulerAdvanced.ConfirmOnMasterAsync(IEnumerable{JobScheduleReceipt})"/>.
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
