namespace JobMaster.Api.ApiModels;

/// <summary>Query criteria for listing agent connections.</summary>
public class ApiAgentConnectionCriteria
{
    /// <summary>Maximum number of results to return.</summary>
    public int? CountLimit { get; set; }
    /// <summary>Number of results to skip before returning.</summary>
    public int? Offset { get; set; }
    /// <summary>Optional sort specification.</summary>
    public ApiSortByCriteria? SortBy { get; set; }
}
