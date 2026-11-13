using System;
using System.Text.Json;
using JobMaster.Abstractions.Serialization;

namespace JobMaster.Serialization;

/// <summary>
/// The default <see cref="IJobMasterMessageJsonSerializer"/>, used for <c>SetJson</c> / <c>GetJson</c> values
/// when no other serializer is defined.
/// <para>
/// Uses <c>System.Text.Json</c> with its default options: property names match case-sensitively, enums are
/// written as numbers, and unknown properties are ignored. This format stays the same across JobMaster versions,
/// so it can be listed in a <see cref="FallbackJobMasterMessageJsonSerializer"/> to keep reading values written by it.
/// </para>
/// </summary>
public sealed class DefaultJobMasterMessageJsonSerializer : IJobMasterMessageJsonSerializer
{
    /// <inheritdoc />
    public string Serialize<T>(T obj)
    {
        return JsonSerializer.Serialize<T>(obj);
    }

    /// <inheritdoc />
    public T Deserialize<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json) ?? throw new InvalidOperationException("Failed to deserialize JSON");
    }

    /// <inheritdoc />
    public object Deserialize(string json, Type type)
    {
        return JsonSerializer.Deserialize(json, type) ?? throw new InvalidOperationException("Failed to deserialize JSON");
    }
}
