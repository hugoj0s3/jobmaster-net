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
    private RecurringScheduleContext? context;

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
    public RecurringScheduleContext Context => context ??= RecurringScheduleConvertUtil.ToContext(model);

    /// <summary>
    /// Makes sure the schedule is saved on master, saving it now if it isn't there yet. When it returns, the schedule
    /// is on master. Returns right away when <see cref="Status"/> is <see cref="ScheduleReceiptStatus.StoredOnMaster"/>.
    /// <para>
    /// Rarely needed: if your agent connections are healthy, you don't need to call it at all. A confirmed schedule's
    /// first run can be delayed by up to about 10 minutes, so avoid it for schedules that recur at short intervals
    /// (under 10 minutes). To confirm many schedules, use
    /// <see cref="IJobMasterSchedulerAdvanced.ConfirmOnMasterAsync(IEnumerable{RecurringScheduleReceipt})"/>.
    /// </para>
    /// <para>
    /// If it throws, the schedule may or may not be saved; calling it again is safe and never creates a second copy.
    /// Confirming after the cluster's data retention has removed a cancelled or finished schedule saves it again as
    /// active. Be careful calling it when the data retention is short: the shorter it is, the more likely a late
    /// confirm brings a removed schedule back.
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
