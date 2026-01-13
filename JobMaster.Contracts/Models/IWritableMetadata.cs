namespace JobMaster.Contracts.Models;

public interface IWritableMetadata
{
    public IWritableMetadata SetStringValue(string key, string value);
    public IWritableMetadata SetIntValue(string key, int value);
    public IWritableMetadata SetLongValue(string key, long value);
    public IWritableMetadata SetCharValue(string key, char value);
    public IWritableMetadata SetBoolValue(string key, bool value);
    public IWritableMetadata SetDoubleValue(string key, double value);
    public IWritableMetadata SetUtcDateTimeValue(string key, DateTime value);
    public IWritableMetadata SetByteArrayValue(string key, byte[] value);
    public IWritableMetadata SetDecimalValue(string key, decimal value);
    public IWritableMetadata SetShortValue(string key, short value);
    public IWritableMetadata SetByteValue(string key, byte value);
    public IWritableMetadata SetGuidValue(string key, Guid value);
    public IWritableMetadata SetEnumValue<TEnum>(string key, TEnum value) where TEnum : struct, Enum;
    
    public IDictionary<string, object?> ToDictionary();
    public IReadableMetadata ToReadable();
}

public static class WritableMetadata 
{
    public static IWritableMetadata New() => new Metadata();
}
