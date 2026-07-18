using System.Text.Json;
using System.Text.Json.Serialization;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.Api.ApiModels;

internal static class ApiGenericRecordValueFilterMappings
{
    internal static GenericRecordValueFilter ToDomainModel(this ApiGenericRecordValueFilter model)
    {
        return new GenericRecordValueFilter
        {
            Key = model.Key,
            Operation = model.Operation.ToDomainModel(),
            Value = UnwrapJsonElement(model.Value),
            Values = model.Values?.Select(UnwrapJsonElement).ToList(),
        };
    }

    /// <summary>
    /// Value/Values are declared as <c>object?</c> so the API can accept any scalar type, but that
    /// means System.Text.Json deserializes them as <see cref="JsonElement"/> rather than a plain
    /// CLR primitive. Left as-is, a JsonElement flows all the way into GenericRecordSqlUtil's
    /// type-based column/parameter selection, which doesn't recognize it (falls through to the
    /// text column) and then fails at the Dapper layer, which has no type mapping for JsonElement.
    /// Unwrap here, once, at the API boundary, so everything downstream sees real CLR types.
    /// </summary>
    private static object? UnwrapJsonElement(object? value)
    {
        if (value is not JsonElement element)
        {
            return value;
        }

        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var longValue) ? longValue : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => value
        };
    }

    internal static GenericFilterOperation ToDomainModel(this ApiGenericFilterOperation operation)
    {
        return (GenericFilterOperation)(int)operation;
    }

    internal static IList<GenericRecordValueFilter> ParseMetadataFiltersJson(string? metadataFiltersJson)
    {
        if (string.IsNullOrWhiteSpace(metadataFiltersJson))
        {
            return new List<GenericRecordValueFilter>();
        }

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
        };
        options.Converters.Add(new JsonStringEnumConverter());

        var models = JsonSerializer.Deserialize<List<ApiGenericRecordValueFilter>>(metadataFiltersJson, options);
        return models?.Select(x => x.ToDomainModel()).ToList() ?? new List<GenericRecordValueFilter>();
    }
}