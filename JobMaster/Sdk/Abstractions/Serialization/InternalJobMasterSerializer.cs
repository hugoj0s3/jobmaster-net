using System.Globalization;
using System.Text.Json;

namespace JobMaster.Sdk.Abstractions.Serialization;

internal static class InternalJobMasterSerializer
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
                //
                // RoundtripKind figures out the Kind from the string itself: a "Z" suffix parses as
                // Utc, an explicit numeric offset parses as Local (converted to this machine's local
                // time, same as DateTime.Parse always does for offset strings), and no timezone info
                // at all parses as Unspecified. Do NOT relabel the result to Utc after the fact --
                // Ticks only means what its Kind says, so slapping DateTimeKind.Utc onto Local-kind
                // ticks silently shifts the instant by the local UTC offset instead of converting it.
                if (s.Contains('T') && DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
                {
                    return dto;
                }

                if (s.Contains('T') && DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto2))
                {
                    return dto2.UtcDateTime;
                }

                // Same idea for Guid: cheap shape pre-check (canonical "8-4-4-4-12" hyphen
                // positions) before bothering to ask TryParse, so ordinary strings skip the
                // parse attempt entirely.
                if (s!.Length == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-'
                    && Guid.TryParse(s, out var gg))
                {
                    return gg;
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