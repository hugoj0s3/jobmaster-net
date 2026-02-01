using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Api.ApiModels;

public class ApiLogItemQueryCriteria
{
    public ApiJobMasterLogLevel? Level { get; set; }
    public ApiJobMasterLogSubjectType? SubjectType { get; set; }
    public string? SubjectId { get; set; }
    
    public DateTime? FromTimestamp { get; set; }
    public DateTime? ToTimestamp { get; set; }
    
    public string? Keyword { get; set; }
    
    public int? CountLimit { get; set; }
    public int? Offset { get; set; }

    internal LogItemQueryCriteria ToDomainCriteria()
    {
        return new LogItemQueryCriteria
        {
            Level = Level.HasValue ? (JobMasterLogLevel)(int)Level.Value : null,
            SubjectType = SubjectType.HasValue ? (JobMasterLogSubjectType)(int)SubjectType.Value : null,
            SubjectId = SubjectId,
            FromTimestamp = FromTimestamp,
            ToTimestamp = ToTimestamp,
            Keyword = Keyword,
            CountLimit = CountLimit ?? 25,
            Offset = Offset ?? 0,
        };
    }
}
