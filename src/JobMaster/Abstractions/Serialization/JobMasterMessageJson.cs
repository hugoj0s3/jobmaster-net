namespace JobMaster.Abstractions.Serialization;

/// <summary>
/// Static façade for the configured <see cref="IJobMasterMessageJsonSerializer"/>.
/// The serializer is set by the framework during startup and must be initialized before any method is called.
/// </summary>
internal static class JobMasterMessageJson
{
    private static IJobMasterMessageJsonSerializer? serializerInstance;
    private static readonly object locker = new();
    internal static void SetMessageSerializer(IJobMasterMessageJsonSerializer serializer)
    {
        lock (locker)
        {
            serializerInstance = serializer;
        }
    }

    internal static void SetMessageSerializerIfNotSet(IJobMasterMessageJsonSerializer serializer)
    {
        lock (locker)
        {
            if (serializerInstance == null)
            {
                serializerInstance = serializer;
            }
        }
    }

    /// <summary>Serializes <paramref name="obj"/> to a JSON string.</summary>
    public static string Serialize<T>(T obj)
    {
        if (serializerInstance == null) throw new InvalidOperationException("Set the serializer first");

        return serializerInstance.Serialize<T>(obj);
    }

    /// <summary>
    /// Deserializes <paramref name="json"/> to <typeparamref name="T"/>.
    /// Throws if the serializer is not configured or the JSON is invalid.
    /// </summary>
    public static T Deserialize<T>(string json)
    {
        if (serializerInstance == null) throw new InvalidOperationException("Set the serializer first");

        return serializerInstance.Deserialize<T>(json);
    }

    /// <summary>
    /// Deserializes <paramref name="json"/> to the specified <paramref name="type"/>.
    /// Throws if the serializer is not configured or the JSON is invalid.
    /// </summary>
    public static object Deserialize(string json, Type type)
    {
        if (serializerInstance == null) throw new InvalidOperationException("Set the serializer first");

        return serializerInstance.Deserialize(json, type);
    }
}
