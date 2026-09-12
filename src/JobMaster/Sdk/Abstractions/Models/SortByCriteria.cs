namespace JobMaster.Sdk.Abstractions.Models;

internal class SortByCriteria
{
    public string Property { get; set; } = string.Empty;
    public bool Ascending { get; set; }
}