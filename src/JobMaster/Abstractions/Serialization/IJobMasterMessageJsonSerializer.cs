namespace JobMaster.Abstractions.Serialization;

/// <summary>
/// Defines the JSON serializer used by JobMaster to serialize and deserialize job messages.
/// Register a custom implementation via the cluster configuration to override the default.
/// </summary>
public interface IJobMasterMessageJsonSerializer
{
    /// <summary>Serializes <paramref name="obj"/> to a JSON string.</summary>
    string Serialize<T>(T obj);

    /// <summary>
    /// Deserializes <paramref name="json"/> to <typeparamref name="T"/>.
    /// Throws if the JSON is null, empty, or malformed.
    /// </summary>
    T Deserialize<T>(string json);

    /// <summary>
    /// Deserializes <paramref name="json"/> to <typeparamref name="T"/>.
    /// Returns <c>default</c> instead of throwing on failure.
    /// </summary>
    T? TryDeserialize<T>(string json);

    /// <summary>
    /// Deserializes <paramref name="json"/> to the specified <paramref name="type"/>.
    /// Throws if the JSON is null, empty, or malformed.
    /// </summary>
    object Deserialize(string json, Type type);

    /// <summary>
    /// Deserializes <paramref name="json"/> to the specified <paramref name="type"/>.
    /// Returns <c>null</c> instead of throwing on failure.
    /// </summary>
    object? TryDeserialize(string json, Type type);
}
