namespace JobMaster.Api.ApiModels;

public class ApiHostCriteria
{
    public int? CountLimit { get; set; }
    public int? Offset { get; set; }
    public ApiSortByCriteria? SortBy { get; set; }
}
