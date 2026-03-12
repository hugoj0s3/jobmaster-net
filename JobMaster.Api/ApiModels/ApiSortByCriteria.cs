namespace JobMaster.Api.ApiModels;

public class ApiSortByCriteria
{
    public string Property { get; set; } = string.Empty;
    public bool Ascending { get; set; }
}