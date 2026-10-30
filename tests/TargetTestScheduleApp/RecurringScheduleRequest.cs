namespace TargetTestScheduleApp;

public sealed record RecurringScheduleRequest(string? ClusterId, string ExpressionTypeId, string Expression, string TestIdentifier, bool? ConfirmOnMaster = null, bool? CancelFromReceipt = null);

/// <summary>Cancelled: <c>receipt.CancelAsync()</c> returned true (false unless CancelFromReceipt).</summary>
public sealed record RecurringScheduleResponse(Guid RecurringScheduleId, bool AcceptedOnTransport, bool Cancelled);
