namespace JobMaster.Sdk.Abstractions.Models.Logs;

public class LogItem
{
    public string ClusterId { get; set; } = string.Empty;
    public JobMasterLogLevel Level { get; set; }
    public string Message { get; set; } = string.Empty;
    public JobMasterLogSubjectType? SubjectType { get; set; }
    public string? SubjectId { get; set; }
    public DateTime TimestampUtc { get; set; }
    public string? Host { get; set; }

    public string? SourceMember { get; set; }
    public string? SourceFile { get; set; }
    public int? SourceLine { get; set; }
    
    public override string ToString()
    {
        return $"{TimestampUtc:O} [{Level}] {SubjectType}/{SubjectId} | {SourceMember} ({SourceFile}:{SourceLine}) | {Message} | {Host}";
    }
}

public class LogItemQueryCriteria
{
    public JobMasterLogLevel? Level { get; set; }
    public JobMasterLogSubjectType? SubjectType { get; set; }
    public string? SubjectId { get; set; }
    
    public DateTime? FromTimestamp { get; set; }
    public DateTime? ToTimestamp { get; set; }
    
    public string? Keyword { get; set; }
    
    public int CountLimit { get; set; } = 100;
    public int Offset { get; set; }
}