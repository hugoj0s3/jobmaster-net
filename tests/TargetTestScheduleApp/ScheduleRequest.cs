namespace TargetTestScheduleApp;

/// <summary>
/// Priority is the raw JobMasterPriority underlying int value (VeryLow=1 .. Critical=5) -- carried
/// as a plain int across the HTTP boundary since JobMaster.ScenarioTests takes no JobMaster
/// reference and can't share the enum type.
/// </summary>
public sealed record ScheduleRequest(string? ClusterId, int QtyJobs, int? AfterSeconds, string TestIdentifier, int? Priority = null, bool? InjectFailure = null, int? MaxNumberOfRetries = null, bool? ConfirmOnMaster = null, bool? CancelFromReceipt = null, bool? CancelFromId = null);

/// <summary>
/// AcceptedOnTransportCount: receipts that were handed to an agent connection (not stored on master).
/// CancelledJobIds: jobs whose cancel returned true (<c>receipt.CancelAsync()</c> for CancelFromReceipt,
/// <c>scheduler.CancelJobAsync(id, clusterId)</c> for CancelFromId; empty when neither is set).
/// </summary>
public sealed record ScheduleResponse(List<Guid> JobIds, int AcceptedOnTransportCount, List<Guid> CancelledJobIds);
