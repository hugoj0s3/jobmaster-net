namespace JobMaster.Contracts.Models;

public class Metadata : IWritableMetadata, IReadableMetadata
{
    private readonly KeyValueBag bag;
    
    public static Metadata Empty => new Metadata();

    public Metadata()
    {
        bag = new KeyValueBag();
    }

    public Metadata(IDictionary<string, object?> values)
    {
        bag = new KeyValueBag(values);
    }

    // ========= Write API =========
    public IWritableMetadata SetStringValue(string key, string value) { bag.SetStringValue(key, value); return this; }
    public IWritableMetadata SetIntValue(string key, int value) { bag.SetIntValue(key, value); return this; }
    public IWritableMetadata SetLongValue(string key, long value) { bag.SetLongValue(key, value); return this; }
    public IWritableMetadata SetCharValue(string key, char value) { bag.SetCharValue(key, value); return this; }
    public IWritableMetadata SetBoolValue(string key, bool value) { bag.SetBoolValue(key, value); return this; }
    public IWritableMetadata SetDoubleValue(string key, double value) { bag.SetDoubleValue(key, value); return this; }
    public IWritableMetadata SetUtcDateTimeValue(string key, DateTime value) { bag.SetUtcDateTimeValue(key, value); return this; }
    public IWritableMetadata SetByteArrayValue(string key, byte[] value) { bag.SetByteArrayValue(key, value); return this; }
    public IWritableMetadata SetDecimalValue(string key, decimal value) { bag.SetDecimalValue(key, value); return this; }
    public IWritableMetadata SetShortValue(string key, short value) { bag.SetShortValue(key, value); return this; }
    public IWritableMetadata SetByteValue(string key, byte value) { bag.SetByteValue(key, value); return this; }
    public IWritableMetadata SetGuidValue(string key, Guid value) { bag.SetGuidValue(key, value); return this; }
    public IWritableMetadata SetEnumValue<TEnum>(string key, TEnum value) where TEnum : struct, Enum { bag.SetEnumValue(key, value); return this; }

    // ========= Read API (non-nullable results) =========
    public string GetStringValue(string key) => bag.GetStringValue(key);
    public int GetIntValue(string key) => bag.GetIntValue(key);
    public long GetLongValue(string key) => bag.GetLongValue(key);
    public char GetCharValue(string key) => bag.GetCharValue(key);
    public bool GetBoolValue(string key) => bag.GetBoolValue(key);
    public double GetDoubleValue(string key) => bag.GetDoubleValue(key);
    public DateTime GetDateTimeValue(string key) => bag.GetDateTimeValue(key);
    public byte[] GetByteArrayValue(string key) => bag.GetByteArrayValue(key);
    public decimal GetDecimalValue(string key) => bag.GetDecimalValue(key);
    public short GetShortValue(string key) => bag.GetShortValue(key);
    public byte GetByteValue(string key) => bag.GetByteValue(key);
    public Guid GetGuidValue(string key) => bag.GetGuidValue(key);
    public TEnum GetEnumValue<TEnum>(string key) where TEnum : struct, Enum { return bag.GetEnumValue<TEnum>(key); }
    
    // ========= Read API (nullable results) =========
    public string? TryGetStringValue(string key) => bag.TryGetStringValue(key);
    public int? TryGetIntValue(string key) => bag.TryGetIntValue(key);
    public long? TryGetLongValue(string key) => bag.TryGetLongValue(key);
    public char? TryGetCharValue(string key) => bag.TryGetCharValue(key);
    public bool? TryGetBoolValue(string key) => bag.TryGetBoolValue(key);
    public double? TryGetDoubleValue(string key) => bag.TryGetDoubleValue(key);
    public DateTime? TryGetDateTimeValue(string key) => bag.TryGetDateTimeValue(key);
    public byte[]? TryGetByteArrayValue(string key) => bag.TryGetByteArrayValue(key);
    public decimal? TryGetDecimalValue(string key) => bag.TryGetDecimalValue(key);
    public short? TryGetShortValue(string key) => bag.TryGetShortValue(key);
    public byte? TryGetByteValue(string key) => bag.TryGetByteValue(key);
    public Guid? TryGetGuidValue(string key) => bag.TryGetGuidValue(key);
    public TEnum? TryGetEnumValue<TEnum>(string key) where TEnum : struct, Enum { return bag.TryGetEnumValue<TEnum>(key); }
    
    public IDictionary<string, object?> ToDictionary() => bag.ToDictionary();
    public IReadableMetadata ToReadable() => this;
}