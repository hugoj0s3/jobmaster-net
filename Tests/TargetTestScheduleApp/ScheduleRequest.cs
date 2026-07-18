namespace TargetTestScheduleApp;

/// <summary>
/// Priority is the raw JobMasterPriority underlying int value (VeryLow=1 .. Critical=5) -- carried
/// as a plain int across the HTTP boundary since JobMaster.ScenarioTests takes no JobMaster
/// reference and can't share the enum type.
/// </summary>
public sealed record ScheduleRequest(string? ClusterId, int QtyJobs, int? AfterSeconds, string TestIdentifier, int? Priority = null);

public sealed record ScheduleResponse(List<Guid> JobIds);
