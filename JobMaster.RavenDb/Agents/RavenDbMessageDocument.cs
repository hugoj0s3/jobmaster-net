namespace JobMaster.RavenDb.Agents;

internal sealed class RavenDbMessageDocument
{
    // Deliberately not named "Id" -- collides with RavenDB's document-identity convention (same issue
    // found and fixed on RavenDbLogDocument.LogId).
    public string MessageId { get; set; } = string.Empty;
    public string BucketAddressId { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTime ReferenceTime { get; set; }
    public string CorrelationId { get; set; } = string.Empty;
    public DateTime EnqueuedAt { get; set; }
}
