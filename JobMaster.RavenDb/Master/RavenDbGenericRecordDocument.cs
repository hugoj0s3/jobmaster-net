namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbGenericRecordDocument
{
    public string ClusterId { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public string EntryId { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public DateTime? ExpiresAt { get; set; }

    // JSON-native types (string, bool, long/int, double) round-trip correctly through a loosely-typed
    // `object` dictionary field. DateTime/Guid/decimal don't -- RavenDB serializes them as plain JSON
    // strings/numbers with no type hint for a generic `object` deserialization target to reconstruct the
    // original CLR type (a DateTime comes back as a bare ISO string, indistinguishable from a real string
    // value). So they get their own strongly-typed buckets instead, mirroring why SQL's EAV value table
    // has separate typed columns per value -- same problem, dictionaries instead of columns.
    public IDictionary<string, object?> Values { get; set; } = new Dictionary<string, object?>();
    public IDictionary<string, DateTime> DateTimeValues { get; set; } = new Dictionary<string, DateTime>();
    public IDictionary<string, Guid> GuidValues { get; set; } = new Dictionary<string, Guid>();
    public IDictionary<string, decimal> DecimalValues { get; set; } = new Dictionary<string, decimal>();
}
