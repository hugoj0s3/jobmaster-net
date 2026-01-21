namespace JobMaster.Abstractions.Serialization;

public interface IJobMasterMessageJsonSerializer
{
    string Serialize<T>(T obj);
    T Deserialize<T>(string json);
    T? TryDeserialize<T>(string json);
    object Deserialize(string json, Type type);
    object? TryDeserialize(string json, Type type);
}