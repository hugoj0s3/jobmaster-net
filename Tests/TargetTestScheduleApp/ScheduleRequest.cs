namespace TargetTestScheduleApp;

public sealed record ScheduleRequest(string? ClusterId, int QtyJobs, int? AfterSeconds, string TestIdentifier);

public sealed record ScheduleResponse(List<Guid> JobIds);
