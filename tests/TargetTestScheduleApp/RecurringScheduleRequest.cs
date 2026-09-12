namespace TargetTestScheduleApp;

public sealed record RecurringScheduleRequest(string? ClusterId, string ExpressionTypeId, string Expression, string TestIdentifier);

public sealed record RecurringScheduleResponse(Guid RecurringScheduleId);
