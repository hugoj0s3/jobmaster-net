using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using JobMaster.Abstractions.Serialization;

namespace JobMaster.Serialization;

/// <summary>
/// An <see cref="IJobMasterMessageJsonSerializer"/> based on <c>System.Text.Json</c>.
/// <c>new SystemTextJsonMessageSerializer()</c> is the default for <c>SetJson</c> / <c>GetJson</c> values
/// when no other serializer is defined.
/// <para>
/// It uses <c>System.Text.Json</c>'s default options: property names match case-sensitively, enums are written as
/// numbers, and unknown properties are ignored. This format stays the same across JobMaster versions, so it can be
/// listed in a <see cref="FallbackMessageJsonSerializer"/> to keep reading values written by it.
/// </para>
/// </summary>
public sealed class SystemTextJsonMessageSerializer : IJobMasterMessageJsonSerializer
{
    private static readonly JsonSerializerOptions StrictReadOptions = new()
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow
    };

    private readonly JsonSerializerOptions? readOptions;

    /// <summary>Creates the serializer.</summary>
    /// <param name="rejectUnknownProperties">
    /// When <c>true</c>, reading JSON with a property the target type doesn't have throws instead of ignoring it.
    /// Writes are unchanged. Use it when this serializer is followed by others in a
    /// <see cref="FallbackMessageJsonSerializer"/>, so JSON written in another format (for example camelCase)
    /// moves on to the next serializer instead of being read as an object with empty properties.
    /// Don't use it as the last or only serializer: stored values with a property since removed from the class would fail.
    /// </param>
    public SystemTextJsonMessageSerializer(bool rejectUnknownProperties = false)
    {
        readOptions = rejectUnknownProperties ? StrictReadOptions : null;
    }

    /// <inheritdoc />
    public string Serialize<T>(T obj)
    {
        return JsonSerializer.Serialize<T>(obj);
    }

    /// <inheritdoc />
    public T Deserialize<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json, readOptions) ?? throw new InvalidOperationException("Failed to deserialize JSON");
    }

    /// <inheritdoc />
    public object Deserialize(string json, Type type)
    {
        return JsonSerializer.Deserialize(json, type, readOptions) ?? throw new InvalidOperationException("Failed to deserialize JSON");
    }
}
