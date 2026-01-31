using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Api.ApiModels;

public class ApiLogItem
{
    public string Id { get; set; } = string.Empty;
    
    public string ClusterId { get; set; } = string.Empty;
    public ApiJobMasterLogLevel Level { get; set; }
    public string Message { get; set; } = string.Empty;
    public ApiJobMasterLogSubjectType? SubjectType { get; set; }
    public string? SubjectId { get; set; }
    public DateTime TimestampUtc { get; set; }
    public string? Host { get; set; }

    public string? SourceMember { get; set; }
    public string? SourceFile { get; set; }
    public int? SourceLine { get; set; }

    // Reduce the payload size.
    // TODO ideally it should be done at repository level.
    // One Idea:
    //    - Create new field 'Summary' that will contain the first 100 characters of the message.
    //    - On Generic repository create criteria that does not include certain fields.
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
            SubjectType = model.SubjectType.HasValue
                ? (ApiJobMasterLogSubjectType)(int)model.SubjectType.Value
                : null,
            SubjectId = model.SubjectId,
            TimestampUtc = model.TimestampUtc,
            Host = model.Host,
            SourceMember = model.SourceMember,
            SourceFile = model.SourceFile,
            SourceLine = model.SourceLine,
        };
    }
}