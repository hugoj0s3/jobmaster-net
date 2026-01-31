namespace JobMaster.Api.ApiModels;

public class ApiGenericRecordValueFilter
{
    public string Key { get; set; } = string.Empty;

    public ApiGenericFilterOperation Operation { get; set; }

    public object? Value { get; set; }

    public IReadOnlyList<object?>? Values { get; set; }
}