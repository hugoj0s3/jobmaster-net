namespace JobMaster.Abstractions.Models;

/// <summary>Where a scheduled job or recurring schedule was stored when the scheduling call returned.</summary>
public enum ScheduleReceiptStatus
{
    /// <summary>
    /// Accepted by the agent transport and handed to a bucket, not yet saved on master. A worker saves it on master
    /// shortly after.
    /// </summary>
    AcceptedOnTransport,

    /// <summary>Already saved on master.</summary>
    StoredOnMaster,
}
