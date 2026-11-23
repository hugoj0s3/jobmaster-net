namespace JobMaster.Abstractions.Serialization;

/// <summary>
/// Serializes the values stored with <c>SetJson</c> and read with <c>GetJson</c> / <c>TryGetJson</c> on message data.
/// The rest of a job (message data entries, metadata, the job itself) always uses JobMaster's own format.
/// <para>
/// Define it with <c>services.DefineJobMasterMessageJsonSerializer(...)</c>. The default is
/// <c>new JobMaster.Serialization.SystemTextJsonMessageSerializer()</c>.
/// </para>
/// <para>
/// One serializer applies to the whole process, every cluster included. Every process that schedules or runs
/// these jobs must use the same serializer with the same settings, or values written by one can't be read, or are
/// read wrongly, by another. Changing it doesn't convert values already stored; use
/// <c>JobMaster.Serialization.FallbackMessageJsonSerializer</c> to keep reading them.
/// </para>
/// <para>
/// <b>Security:</b> message data can be written by anyone who can schedule jobs or write to the job storage or
/// transport, and it is deserialized on the workers. Never let the JSON choose the type to create: with
/// Newtonsoft.Json, keep <c>TypeNameHandling</c> at <c>None</c>, or restrict types with a <c>SerializationBinder</c>
/// if you need polymorphism. Otherwise a crafted payload can run code on your workers.
/// </para>
/// </summary>
public interface IJobMasterMessageJsonSerializer
{
    /// <summary>Serializes <paramref name="obj"/> to a JSON string.</summary>
    string Serialize<T>(T obj);

    /// <summary>
    /// Deserializes <paramref name="json"/> to <typeparamref name="T"/>.
    /// Must throw if the JSON is null, empty, malformed, or can't be read as <typeparamref name="T"/>, never return
    /// <c>default</c>: JobMaster relies on the exception to report a broken value instead of treating it as missing.
    /// </summary>
    T Deserialize<T>(string json);

    /// <summary>
    /// Deserializes <paramref name="json"/> to the specified <paramref name="type"/>.
    /// Must throw if the JSON is null, empty, malformed, or can't be read as <paramref name="type"/>, never return <c>null</c>.
    /// </summary>
    object Deserialize(string json, Type type);
}
