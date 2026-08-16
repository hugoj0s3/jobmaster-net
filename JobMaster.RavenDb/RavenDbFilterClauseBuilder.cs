using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.RavenDb;

/// <summary>
/// Shared RQL predicate builder for <see cref="GenericRecordValueFilter"/>-shaped filters, used by both
/// <c>RavenDbMasterGenericRecordRepository</c> (against <c>Values</c>/<c>DateTimeValues</c>/...) and
/// <c>RavenDbMasterJobsRepository</c> (against <c>MetadataValues</c>/<c>MetadataDateTimeValues</c>/...).
/// </summary>
internal static class RavenDbFilterClauseBuilder
{
    /// <param name="fieldPrefix">
    /// Prefix on the 4 typed-bucket field names (e.g. "Metadata" for Jobs' <c>MetadataValues</c> vs
    /// GenericRecords' bare <c>Values</c>).
    /// </param>
    /// <param name="paramPrefix">Prefix for the generated RQL parameter name, must be unique per caller.</param>
    public static (string Clause, string? ParamName, object? ParamValue) Build(
        GenericRecordValueFilter filter, int index, string paramPrefix, string fieldPrefix = "")
    {
        // RQL has no bracket-index syntax for nested-object field access (`Values['key']` is a parse
        // error -- RQL only understands dot-path field access), so this only works for keys that are
        // valid identifiers; caller-supplied keys with spaces/hyphens/leading digits aren't queryable
        // this way -- a known limitation, not attempted here.
        //
        // Which bucket dictionary a key lives in (Values vs DateTimeValues/GuidValues/DecimalValues) is
        // determined by the FILTER's own value type, mirroring exactly how each write path dispatches by
        // the stored value's runtime type -- the two must always agree.
        var sampleValue = filter.Operation == GenericFilterOperation.In
            ? filter.Values?.FirstOrDefault(v => v != null)
            : filter.Value;
        var bucket = sampleValue switch
        {
            DateTime => $"{fieldPrefix}DateTimeValues",
            Guid => $"{fieldPrefix}GuidValues",
            decimal => $"{fieldPrefix}DecimalValues",
            _ => $"{fieldPrefix}Values",
        };
        var field = $"e.{bucket}.{filter.Key}";
        var paramName = $"{paramPrefix}{index}";

        switch (filter.Operation)
        {
            case GenericFilterOperation.Eq:
                return ($"{field} = ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Neq:
                // Must also match documents that don't have this key at all -- a missing field access
                // in RQL yields null/undefined, and null != a non-null value is true, same as JS.
                return ($"{field} != ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.In:
                return ($"{field} in (${paramName})", paramName, filter.Values?.ToArray() ?? Array.Empty<object?>());
            case GenericFilterOperation.Gt:
                return ($"{field} > ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Gte:
                return ($"{field} >= ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Lt:
                return ($"{field} < ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Lte:
                return ($"{field} <= ${paramName}", paramName, filter.Value);
            case GenericFilterOperation.Contains:
                // No plain substring operator in RQL outside full-text search (which is token-based, not
                // substring-based) -- regex() against an escaped literal gives exact substring semantics.
                return ($"regex({field}, ${paramName})", paramName, System.Text.RegularExpressions.Regex.Escape(filter.Value?.ToString() ?? string.Empty));
            case GenericFilterOperation.StartsWith:
                return ($"startsWith({field}, ${paramName})", paramName, filter.Value?.ToString());
            case GenericFilterOperation.EndsWith:
                return ($"endsWith({field}, ${paramName})", paramName, filter.Value?.ToString());
            default:
                throw new NotSupportedException($"Unsupported filter operation: {filter.Operation}");
        }
    }
}
