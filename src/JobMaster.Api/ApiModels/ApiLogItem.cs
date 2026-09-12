using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Api.ApiModels;

/// <summary>Represents a structured log item as returned by the API.</summary>
public class ApiLogItem
{
    /// <summary>Unique identifier of the log entry (base64url-encoded GUID).</summary>
    public string Id { get; set; } = string.Empty;
    /// <summary>Identifier of the cluster that produced this log entry.</summary>
    public string ClusterId { get; set; } = string.Empty;
    /// <summary>Severity level of the log entry.</summary>
    public ApiJobMasterLogLevel Level { get; set; }
    /// <summary>Log message text.</summary>
    public string Message { get; set; } = string.Empty;
    /// <summary>Optional category grouping (e.g. Job, Bucket, Cluster).</summary>
    public ApiJobMasterLogCategory? Category { get; set; }
    /// <summary>Optional reference identifier linking the entry to a specific resource.</summary>
    public string? ReferenceId { get; set; }
    /// <summary>UTC timestamp when the log entry was recorded.</summary>
    public DateTime TimestampUtc { get; set; }
    /// <summary>Host that produced the log entry, if available.</summary>
    public string? Host { get; set; }
    /// <summary>Name of the method or member that produced the log entry.</summary>
    public string? SourceMember { get; set; }
    /// <summary>Source file path that produced the log entry.</summary>
    public string? SourceFile { get; set; }
    /// <summary>Line number in the source file that produced the log entry.</summary>
    public int? SourceLine { get; set; }

    // Reduce the payload size.
    // TODO ideally it should be done at repository level.
    // One Idea:
    //    - Create new field 'Summary' that will contain the first 100 characters of the message.
    //    - On Generic repository create criteria that does not include certain fields.
    /// <summary>Truncates <see cref="Message"/> to 100 characters to reduce payload size.</summary>
    public void CutMessage()
    {
        if (Message.Length > 100)
        {
            Message = Message.Substring(0, 100) + "...";
        }
    }

    internal static ApiLogItem FromDomain(LogItem model)
    {
        return new ApiLogItem
        {
            Id = model.Id.ToBase64(),
            ClusterId = model.ClusterId,
            Level = (ApiJobMasterLogLevel)(int)model.Level,
            Message = model.Message,
            Category = model.Category.HasValue
                ? (ApiJobMasterLogCategory)(int)model.Category.Value
                : null,
            ReferenceId = model.ReferenceId,
            TimestampUtc = model.TimestampUtc,
            Host = model.Host,
            SourceMember = model.SourceMember,
            SourceFile = model.SourceFile,
            SourceLine = model.SourceLine,
        };
    }
}
