using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Api.ApiModels;

public class ApiLogItemQueryCriteria
{
    public ApiJobMasterLogLevel? Level { get; set; }
    public ApiJobMasterLogCategory? Category { get; set; }
    public string? ReferenceId { get; set; }

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
            Category = Category.HasValue ? (JobMasterLogCategory)(int)Category.Value : null,
            ReferenceId = ReferenceId,
            FromTimestamp = FromTimestamp,
            ToTimestamp = ToTimestamp,
            Keyword = Keyword,
            CountLimit = CountLimit ?? 25,
            Offset = Offset ?? 0,
        };
    }
}
