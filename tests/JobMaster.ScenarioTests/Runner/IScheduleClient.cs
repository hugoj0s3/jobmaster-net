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
        CancellationToken ct = default);
}

public sealed record ScheduleClientResult(List<Guid> JobIds);
