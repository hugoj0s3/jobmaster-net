using System.Globalization;
using System.Text.Json;

namespace JobMaster.Sdk.Abstractions.Serialization;

public static class InternalJobMasterSerializer
{
    public static string Serialize<T>(T value)
    {
        return JsonSerializer.Serialize(value);
    }

    public static T? TryDeserialize<T>(string json)
    {
        try
        {
            return JsonSerializer.Deserialize<T>(json);
        }
        catch (JsonException)
        {
            return default;
        }
        catch (NotSupportedException)
        {
            return default; 
        }
    }

    public static T Deserialize<T>(string json)
    {
        if (typeof(T) == typeof(Dictionary<string, object?>))
            return (T)(object)DeserializeToDictionary(json);
        
        if (typeof(T) == typeof(IDictionary<string, object?>))
            return (T)(object)DeserializeToDictionary(json);
        
        return JsonSerializer.Deserialize<T>(json)
               ?? throw new JsonException($"Failed to deserialize JSON to {typeof(T).Name}");
    }

    public static object? TryDeserialize(string json, Type t)
    {
        try
        {
            return JsonSerializer.Deserialize(json, t);
        }
        catch (JsonException)
        {
            return null;
        }
        catch (NotSupportedException)
        {
            return null; 
        }
    }
    
    private static Dictionary<string, object?> DeserializeToDictionary(string json)
    {
        var unnormalizedResult = JsonSerializer.Deserialize<Dictionary<string, object?>>(json) ?? throw new JsonException($"Failed to deserialize JSON to Dictionary<string, object?>");
        
        var normalizedResult = new Dictionary<string, object?>();
        foreach (var kvp in unnormalizedResult)
        {
            normalizedResult.Add(kvp.Key, NormalizeValue(kvp.Value, kvp.Key));
        }
        
        return normalizedResult;
    }
    
    private static object? NormalizeValue(object? value, string key)
    {
        if (value is null)
            return null;

        if (value is JsonElement je)
            return FromJsonElement(je, key);

        // Already some primitive type (string, long, bool, etc.) — keep as is.
        return value;
    }

    private static object? FromJsonElement(JsonElement je, string key)
    {
        switch (je.ValueKind)
        {
            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                return null;

            case JsonValueKind.True:
                return true;

            case JsonValueKind.False:
                return false;

            case JsonValueKind.String:
            {
                var s = je.GetString();
                if (string.IsNullOrEmpty(s))
                {
                    return s;
                }

                // If the JSON string looks like an ISO-8601 timestamp, normalize it to a DateTime.
                // This enables typed storage/querying for metadata DateTime filters.
                // We keep other strings as-is.
                if (s.Contains('T') && DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
                {
                    return new DateTime(dto.Ticks, DateTimeKind.Utc);
                }
                
                if (s.Contains('T') && DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto2))
                {
                    return dto2.UtcDateTime;
                }

                return s;
            }

            case JsonValueKind.Number:
                // Prefer "whole number" + "decimal" model.
                if (je.TryGetInt64(out var l))
                    return l; // whole number

                if (je.TryGetDecimal(out var d))
                    return d; // fractional

                // Extremely rare fallback (you can also throw here if you want to be stricter)
                return je.GetDouble();

            case JsonValueKind.Object:
            case JsonValueKind.Array:
            default:
                return je.GetRawText();
        }
    }
}