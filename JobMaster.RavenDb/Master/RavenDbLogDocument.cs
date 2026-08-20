using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.RavenDb.Master;

internal sealed class RavenDbLogDocument
{
    // Deliberately not named "Id" -- RavenDB auto-populates any property literally named Id with its own
    // document-ID string, which throws against a non-string Guid like this one.
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
