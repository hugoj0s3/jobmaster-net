namespace JobMaster.Abstractions.Models;

public interface IWriteableMessageData
{
    public IWriteableMessageData SetStringValue(string key, string value);
    public IWriteableMessageData SetIntValue(string key, int value);
    public IWriteableMessageData SetLongValue(string key, long value);
    public IWriteableMessageData SetCharValue(string key, char value);
    public IWriteableMessageData SetBoolValue(string key, bool value);
    public IWriteableMessageData SetDoubleValue(string key, double value);
    public IWriteableMessageData SetDateTimeValue(string key, DateTime value);
    public IWriteableMessageData SetEnumValue<TEnum>(string key, TEnum value) where TEnum : struct, Enum;
    public IWriteableMessageData SetByteArrayValue(string key, byte[] value);
    public IWriteableMessageData SetDecimalValue(string key, decimal value);
    public IWriteableMessageData SetShortValue(string key, short value);
    public IWriteableMessageData SetByteValue(string key, byte value);
    public IWriteableMessageData SetGuidValue(string key, Guid value);
    public IWriteableMessageData SetJson<T>(string key, T? value) where T : new();
    
    public IDictionary<string, object?> ToDictionary();
    public IReadableMessageData ToReadable();
}