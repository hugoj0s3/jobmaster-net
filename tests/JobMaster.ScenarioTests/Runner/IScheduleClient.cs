namespace JobMaster.ScenarioTests.Runner;

public interface IScheduleClient
{
    Task<ScheduleClientResult> ScheduleAsync(
        string handlerType,
        string testIdentifier,
        int qtyJobs = 1,
        string? clusterId = null,
        int? afterSeconds = null,
        int? priority = null,
        bool? injectFailure = null,
        int? maxNumberOfRetries = null,
        bool? confirmOnMaster = null,
        bool? cancelFromReceipt = null,
        CancellationToken ct = default);
}

// AcceptedOnTransportCount / CancelledJobIds: only returned by TargetTestScheduleApp (default when the app doesn't send them).
public sealed record ScheduleClientResult(List<Guid> JobIds, int AcceptedOnTransportCount = 0, List<Guid>? CancelledJobIds = null);
