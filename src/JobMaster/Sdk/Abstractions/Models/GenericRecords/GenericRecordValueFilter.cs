namespace JobMaster.Sdk.Abstractions.Models.GenericRecords;

public sealed class GenericRecordValueFilter
{
    public string Key { get; set; } = string.Empty;
    public GenericFilterOperation Operation { get; set; } = GenericFilterOperation.Eq;

    /// <summary>
    /// Single value for Eq, Neq, Gt, Gte, Lt, Lte, Contains, StartsWith, EndsWith
    /// </summary>
    public object? Value { get; set; }

    /// <summary>
    /// List of values for In
    /// </summary>
    public IReadOnlyList<object?>? Values { get; set; }
}