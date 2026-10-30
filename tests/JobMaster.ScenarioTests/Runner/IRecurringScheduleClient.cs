namespace JobMaster.ScenarioTests.Runner;

public interface IRecurringScheduleClient
{
    Task<RecurringScheduleClientResult> CreateRecurringAsync(
        string handlerType,
        string expressionTypeId,
        string expression,
        string testIdentifier,
        string? clusterId = null,
        bool? confirmOnMaster = null,
        bool? cancelFromReceipt = null,
        CancellationToken ct = default);

    Task CancelRecurringAsync(
        Guid recurringScheduleId,
        string? clusterId = null,
        CancellationToken ct = default);
}

// AcceptedOnTransport / Cancelled: only returned by TargetTestScheduleApp (false when the app doesn't send them).
public sealed record RecurringScheduleClientResult(Guid RecurringScheduleId, bool AcceptedOnTransport = false, bool Cancelled = false);
