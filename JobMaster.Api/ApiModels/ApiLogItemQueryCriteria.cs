using JobMaster.Sdk.Abstractions.Models.Logs;

namespace JobMaster.Api.ApiModels;

/// <summary>Query criteria for filtering and paginating log items.</summary>
public class ApiLogItemQueryCriteria
{
    /// <summary>Filter by log severity level.</summary>
    public ApiJobMasterLogLevel? Level { get; set; }
    /// <summary>Filter by log category.</summary>
    public ApiJobMasterLogCategory? Category { get; set; }
    /// <summary>
    /// Filter by reference identifier as a raw string (matched exactly against the stored value).
    /// Use <see cref="ReferenceGuid"/> instead when the reference ID is a GUID.
    /// </summary>
    public string? ReferenceId { get; set; }
    /// <summary>
    /// Filter by a GUID-typed reference identifier. Accepts either standard GUID format
    /// (<c>xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx</c>) or the compact base64url form (22 characters).
    /// When set, takes precedence over <see cref="ReferenceId"/> and normalises the value to the
    /// storage format before querying.
    /// </summary>
    public string? ReferenceGuid { get; set; }
    
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
        var criteria = new LogItemQueryCriteria
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

        if (!string.IsNullOrEmpty(ReferenceGuid))
        {
            var guid = ReferenceGuid.ParseFlexible();
            criteria.ReferenceId = guid.ToString("N");
        }

        return criteria;
    }
}
