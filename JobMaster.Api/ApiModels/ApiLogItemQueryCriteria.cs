using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Api.ApiModels;

/// <summary>Query criteria for filtering and paginating log items.</summary>
public class ApiLogItemQueryCriteria
{
    /// <summary>Filter by log severity level.</summary>
    public ApiJobMasterLogLevel? Level { get; set; }
    /// <summary>Filter by log category.</summary>
    public ApiJobMasterLogCategory? Category { get; set; }
    /// <summary>Filter by reference identifier (e.g. job or schedule ID).</summary>
    public string? ReferenceId { get; set; }
    /// <summary>Lower bound for the log entry timestamp (inclusive).</summary>
    public DateTime? FromTimestamp { get; set; }
    /// <summary>Upper bound for the log entry timestamp (inclusive).</summary>
    public DateTime? ToTimestamp { get; set; }
    /// <summary>Full-text keyword to search within log messages.</summary>
    public string? Keyword { get; set; }
    /// <summary>Maximum number of results to return. Defaults to 25.</summary>
    public int? CountLimit { get; set; }
    /// <summary>Number of results to skip before returning.</summary>
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
