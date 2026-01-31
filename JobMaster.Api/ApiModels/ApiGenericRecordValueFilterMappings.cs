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
            Value = model.Value,
            Values = model.Values,
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