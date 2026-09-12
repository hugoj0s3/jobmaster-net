using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Serialization;

namespace JobMaster.Sdk.Abstractions.Jobs;

/// <summary>
/// Serialize/deserialize a <see cref="System.Collections.Generic.IDictionary{TKey,TValue}"/> to/from
/// the JSON string persisted on the raw model -- the exact same shape whether the dictionary came
/// from <c>Metadata.ToDictionary()</c> or <c>MsgData.ToDictionary()</c>, since both are backed by the
/// same underlying <c>KeyValueBag</c>. Shared by <see cref="JobConvertUtil"/> and
/// <see cref="RecurringScheduleConvertUtil"/> so both features flow through one implementation, and so
/// conformance tests can build/read their data through the same code path production code uses instead
/// of a parallel reimplementation that could silently drift from it.
/// </summary>
internal static class KeyValueBagUtil
{
    public static string Serialize(IWritableMetadata? metadata)
        => Serialize(metadata?.ToDictionary());

    public static string Serialize(IWriteableMessageData? msgData)
        => Serialize(msgData?.ToDictionary());

    public static IWritableMetadata DeserializeMetadata(string? metadata)
        => new Metadata(Deserialize(metadata));

    public static IWriteableMessageData DeserializeMessageData(string? msgData)
        => new MessageData(Deserialize(msgData));
    
    private static string Serialize(IDictionary<string, object?>? dict)
        => dict != null ? InternalJobMasterSerializer.Serialize(dict) : "{}";

    private static IDictionary<string, object?> Deserialize(string? json)
        => !string.IsNullOrEmpty(json)
            ? (InternalJobMasterSerializer.Deserialize<Dictionary<string, object?>>(json!) ?? new Dictionary<string, object?>())
            : new Dictionary<string, object?>();
}
