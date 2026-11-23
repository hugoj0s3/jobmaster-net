namespace TargetTestClusterIdApp;

// Same shape as TargetTestScheduleApp's ScheduleRequest/ScheduleResponse (the fields this app doesn't use are
// accepted and ignored) so the scenario side's existing ScheduleClient works unchanged.
public sealed record ScheduleRequest(string? ClusterId, int QtyJobs, string TestIdentifier);

public sealed record ScheduleResponse(List<Guid> JobIds);
