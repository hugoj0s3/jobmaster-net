using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbLogDocument
{
    // Deliberately not named "Id" -- RavenDB's client convention (DocumentConventions.FindIdentityProperty)
    // treats any property literally named Id as the holder for the RavenDB document ID string itself, auto-
    // populating/overwriting it on load/store. Since this is LogItem's own Guid, not RavenDB's document ID,
    // that collision throws ("Cannot set identity value '...' on property 'Id' ... property type is not a
    // string").
    public Guid LogId { get; set; }
    public string ClusterId { get; set; } = string.Empty;
    public JobMasterLogLevel Level { get; set; }
    public string Message { get; set; } = string.Empty;
    public JobMasterLogCategory? Category { get; set; }
    public string? ReferenceId { get; set; }
    public DateTime TimestampUtc { get; set; }
    public string? Host { get; set; }
    public string? SourceMember { get; set; }
    public string? SourceFile { get; set; }
    public int? SourceLine { get; set; }
}
