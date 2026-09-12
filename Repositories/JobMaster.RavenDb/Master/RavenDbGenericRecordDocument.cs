namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbGenericRecordDocument
{
    public string ClusterId { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public string EntryId { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public DateTime? ExpiresAt { get; set; }

    // DateTime/Guid/decimal don't round-trip through a loosely-typed `object` dictionary (RavenDB has no
    // type hint to reconstruct them from their JSON form), so they get their own typed buckets -- same
    // reason SQL's EAV value table has separate typed columns instead of one generic one.
    public IDictionary<string, object?> Values { get; set; } = new Dictionary<string, object?>();
    public IDictionary<string, DateTime> DateTimeValues { get; set; } = new Dictionary<string, DateTime>();
    public IDictionary<string, Guid> GuidValues { get; set; } = new Dictionary<string, Guid>();
    public IDictionary<string, decimal> DecimalValues { get; set; } = new Dictionary<string, decimal>();
}
