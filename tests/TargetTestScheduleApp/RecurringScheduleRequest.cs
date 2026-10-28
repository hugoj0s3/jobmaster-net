namespace TargetTestScheduleApp;

public sealed record RecurringScheduleRequest(string? ClusterId, string ExpressionTypeId, string Expression, string TestIdentifier, bool? ConfirmOnMaster = null);

public sealed record RecurringScheduleResponse(Guid RecurringScheduleId, bool AcceptedOnTransport);
