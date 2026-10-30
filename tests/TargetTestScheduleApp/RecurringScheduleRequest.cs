namespace TargetTestScheduleApp;

public sealed record RecurringScheduleRequest(string? ClusterId, string ExpressionTypeId, string Expression, string TestIdentifier, bool? ConfirmOnMaster = null, bool? CancelFromReceipt = null, bool? CancelFromId = null);

/// <summary>
/// Cancelled: the cancel returned true (<c>receipt.CancelAsync()</c> for CancelFromReceipt,
/// <c>scheduler.CancelRecurringAsync(id, clusterId)</c> for CancelFromId; false when neither is set).
/// </summary>
public sealed record RecurringScheduleResponse(Guid RecurringScheduleId, bool AcceptedOnTransport, bool Cancelled);
