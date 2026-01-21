namespace JobMaster.Abstractions.Serialization;

public static class JobMasterMessageJson
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
    
    public static string Serialize<T>(T obj)
    {
        if (serializerInstance == null) throw new InvalidOperationException("Set the serializer first");
        
        return serializerInstance.Serialize<T>(obj);
    }

    public static T Deserialize<T>(string json)
    {
        if (serializerInstance == null) throw new InvalidOperationException("Set the serializer first");
        
        return serializerInstance.Deserialize<T>(json);
    }
    
    public static T? TryDeserialize<T>(string json)
    {
        if (serializerInstance == null) throw new InvalidOperationException("Set the serializer first");
        
        return serializerInstance.TryDeserialize<T>(json);
    }

    public static object Deserialize(string json, Type type)
    {
        if (serializerInstance == null) throw new InvalidOperationException("Set the serializer first");

        return serializerInstance.Deserialize(json, type);
    }

    public static object? TryDeserialize(string json, Type type)
    {
        if (serializerInstance == null) throw new InvalidOperationException("Set the serializer first");

        return serializerInstance.TryDeserialize(json, type);
    }
}