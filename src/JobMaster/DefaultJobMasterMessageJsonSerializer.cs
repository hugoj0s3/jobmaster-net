using System;
using System.Text.Json;
using JobMaster.Abstractions.Serialization;

namespace JobMaster;

internal class DefaultJobMasterMessageJsonSerializer : IJobMasterMessageJsonSerializer
{
    public string Serialize<T>(T obj)
    {
        return JsonSerializer.Serialize<T>(obj);
    }

    public T Deserialize<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json) ?? throw new InvalidOperationException("Failed to deserialize JSON");
    }

    public T? TryDeserialize<T>(string json)
    {
        try { return JsonSerializer.Deserialize<T>(json); }
        catch (JsonException) { return default; }
        catch (NotSupportedException) { return default; }
    }

    public object Deserialize(string json, Type type)
    {
        return JsonSerializer.Deserialize(json, type) ?? throw new InvalidOperationException("Failed to deserialize JSON");
    }

    public object? TryDeserialize(string json, Type type)
    {
        try { return JsonSerializer.Deserialize(json, type); }
        catch (JsonException) { return null; }
        catch (NotSupportedException) { return null; }
    }
}