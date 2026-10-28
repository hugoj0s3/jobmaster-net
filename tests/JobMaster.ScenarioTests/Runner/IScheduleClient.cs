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
        CancellationToken ct = default);
}

// AcceptedOnTransportCount: only returned by TargetTestScheduleApp (0 when the app doesn't send it).
public sealed record ScheduleClientResult(List<Guid> JobIds, int AcceptedOnTransportCount = 0);
