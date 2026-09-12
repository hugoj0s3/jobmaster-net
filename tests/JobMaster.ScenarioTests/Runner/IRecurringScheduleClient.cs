namespace JobMaster.ScenarioTests.Runner;

public interface IRecurringScheduleClient
{
    Task<RecurringScheduleClientResult> CreateRecurringAsync(
        string handlerType,
        string expressionTypeId,
        string expression,
        string testIdentifier,
        string? clusterId = null,
        CancellationToken ct = default);

    Task CancelRecurringAsync(
        Guid recurringScheduleId,
        string? clusterId = null,
        CancellationToken ct = default);
}

public sealed record RecurringScheduleClientResult(Guid RecurringScheduleId);
