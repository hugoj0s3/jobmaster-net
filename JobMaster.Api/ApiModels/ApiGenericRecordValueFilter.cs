namespace JobMaster.Api.ApiModels;

/// <summary>Defines a single filter condition applied to a generic record value field.</summary>
public class ApiGenericRecordValueFilter
{
    /// <summary>The field key to filter on.</summary>
    public string Key { get; set; } = string.Empty;
    /// <summary>The comparison operation to apply.</summary>
    public ApiGenericFilterOperation Operation { get; set; }
    /// <summary>The scalar value to compare against (used for non-list operations).</summary>
    public object? Value { get; set; }
    /// <summary>The list of values to compare against (used for <see cref="ApiGenericFilterOperation.In"/>).</summary>
    public IReadOnlyList<object?>? Values { get; set; }
}