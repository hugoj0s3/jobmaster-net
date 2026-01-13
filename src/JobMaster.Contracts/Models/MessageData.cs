using System.Globalization;
using System.Text;
using JobMaster.Contracts.Serialization;

namespace JobMaster.Contracts.Models;

internal class MessageData : IReadableMessageData, IWriteableMessageData
{
    public static MessageData Empty => new MessageData();

    private readonly KeyValueBag bag;

    public MessageData()
    {
        bag = new KeyValueBag();
    }

    public MessageData(IDictionary<string, object?> values)
    {
        bag = new KeyValueBag(values);
    }

    // ========= Write API =========
    public IWriteableMessageData SetStringValue(string key, string value) { bag.SetStringValue(key, value); return this; }
    public IWriteableMessageData SetIntValue(string key, int value) { bag.SetIntValue(key, value); return this; }
    public IWriteableMessageData SetLongValue(string key, long value) { bag.SetLongValue(key, value); return this; }
    public IWriteableMessageData SetCharValue(string key, char value) { bag.SetCharValue(key, value); return this; }
    public IWriteableMessageData SetBoolValue(string key, bool value) { bag.SetBoolValue(key, value); return this; }
    public IWriteableMessageData SetDoubleValue(string key, double value) { bag.SetDoubleValue(key, value); return this; }
    public IWriteableMessageData SetDateTimeValue(string key, DateTime value) { bag.SetUtcDateTimeValue(key, value); return this; }
    public IWriteableMessageData SetEnumValue<TEnum>(string key, TEnum value) where TEnum : struct, Enum { bag.SetEnumValue(key, value); return this; }
    public IWriteableMessageData SetByteArrayValue(string key, byte[] value) { bag.SetByteArrayValue(key, value); return this; }
    public IWriteableMessageData SetDecimalValue(string key, decimal value) { bag.SetDecimalValue(key, value); return this; }
    public IWriteableMessageData SetShortValue(string key, short value) { bag.SetShortValue(key, value); return this; }
    public IWriteableMessageData SetByteValue(string key, byte value) { bag.SetByteValue(key, value); return this; }
    public IWriteableMessageData SetGuidValue(string key, Guid value) { bag.SetGuidValue(key, value); return this; }
    public IWriteableMessageData SetJson<T>(string key, T? value) where T : new() { bag.SetJson(key, value); return this; }

    // ========= Read API (TryGet*) =========
    public string? TryGetStringValue(string key) => bag.TryGetStringValue(key);
    public int? TryGetIntValue(string key) => bag.TryGetIntValue(key);
    public long? TryGetLongValue(string key) => bag.TryGetLongValue(key);
    public short? TryGetShortValue(string key) => bag.TryGetShortValue(key);
    public byte? TryGetByteValue(string key) => bag.TryGetByteValue(key);
    public double? TryGetDoubleValue(string key) => bag.TryGetDoubleValue(key);
    public decimal? TryGetDecimalValue(string key) => bag.TryGetDecimalValue(key);
    public bool? TryGetBoolValue(string key) => bag.TryGetBoolValue(key);
    public char? TryGetCharValue(string key) => bag.TryGetCharValue(key);
    public DateTime? TryGetDateTimeValue(string key) => bag.TryGetDateTimeValue(key);
    public Guid? TryGetGuidValue(string key) => bag.TryGetGuidValue(key);
    public byte[]? TryGetByteArrayValue(string key) => bag.TryGetByteArrayValue(key);
    public TEnum? TryGetEnumValue<TEnum>(string key) where TEnum : struct, Enum => bag.TryGetEnumValue<TEnum>(key);
    public T? TryGetJson<T>(string key) where T : new() => bag.TryGetJson<T>(key);

    // ========= Read API (Get*) =========
    public string GetStringValue(string key) => bag.GetStringValue(key);
    public int GetIntValue(string key) => bag.GetIntValue(key);
    public long GetLongValue(string key) => bag.GetLongValue(key);
    public short GetShortValue(string key) => bag.GetShortValue(key);
    public byte GetByteValue(string key) => bag.GetByteValue(key);
    public double GetDoubleValue(string key) => bag.GetDoubleValue(key);
    public decimal GetDecimalValue(string key) => bag.GetDecimalValue(key);
    public bool GetBoolValue(string key) => bag.GetBoolValue(key);
    public char GetCharValue(string key) => bag.GetCharValue(key);
    public DateTime GetDateTimeValue(string key) => bag.GetDateTimeValue(key);
    public Guid GetGuidValue(string key) => bag.GetGuidValue(key);
    public byte[] GetByteArrayValue(string key) => bag.GetByteArrayValue(key);
    public TEnum GetEnumValue<TEnum>(string key) where TEnum : struct, Enum => bag.GetEnumValue<TEnum>(key);
    public T GetJson<T>(string key) where T : new() => bag.GetJson<T>(key);

    public IDictionary<string, object?> ToDictionary() => bag.ToDictionary();

    public IReadableMessageData ToReadable() => new MessageData(this.ToDictionary());
}

public static class WriteableMessageData
{
    public static IWriteableMessageData New() => new MessageData();
}