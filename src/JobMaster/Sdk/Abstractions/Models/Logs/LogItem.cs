namespace JobMaster.Sdk.Abstractions.Models.Logs;

internal class LogItem
{
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

    public Guid Id { get; set; }

    public override string ToString()
    {
        return $"{TimestampUtc:O} [{Level}] {Category}/{ReferenceId} | {SourceMember} ({SourceFile}:{SourceLine}) | {Message} | {Host}";
    }
}

internal class LogItemQueryCriteria
{
    public JobMasterLogLevel? Level { get; set; }
    public JobMasterLogCategory? Category { get; set; }
    public string? ReferenceId { get; set; }

    public DateTime? FromTimestamp { get; set; }
    public DateTime? ToTimestamp { get; set; }

    public string? Keyword { get; set; }

    public int CountLimit { get; set; } = 100;
    public int Offset { get; set; }
}
