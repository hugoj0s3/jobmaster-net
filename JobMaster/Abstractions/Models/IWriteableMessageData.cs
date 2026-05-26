namespace JobMaster.Abstractions.Models;

/// <summary>
/// Fluent builder for writing typed key-value pairs into a job message payload.
/// All setter methods return the same instance to allow method chaining.
/// </summary>
public interface IWriteableMessageData
{
    /// <summary>Stores a <see cref="string"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetStringValue(string key, string value);
    /// <summary>Stores an <see cref="int"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetIntValue(string key, int value);
    /// <summary>Stores a <see cref="long"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetLongValue(string key, long value);
    /// <summary>Stores a <see cref="char"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetCharValue(string key, char value);
    /// <summary>Stores a <see cref="bool"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetBoolValue(string key, bool value);
    /// <summary>Stores a <see cref="double"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetDoubleValue(string key, double value);
    /// <summary>Stores a <see cref="DateTime"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetDateTimeValue(string key, DateTime value);
    /// <summary>Stores an enum value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetEnumValue<TEnum>(string key, TEnum value) where TEnum : struct, Enum;
    /// <summary>Stores a <see cref="byte"/> array value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetByteArrayValue(string key, byte[] value);
    /// <summary>Stores a <see cref="decimal"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetDecimalValue(string key, decimal value);
    /// <summary>Stores a <see cref="short"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetShortValue(string key, short value);
    /// <summary>Stores a <see cref="byte"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetByteValue(string key, byte value);
    /// <summary>Stores a <see cref="Guid"/> value under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetGuidValue(string key, Guid value);
    /// <summary>Serializes <paramref name="value"/> as JSON and stores it under <paramref name="key"/>.</summary>
    public IWriteableMessageData SetJson<T>(string key, T? value) where T : new();

    /// <summary>Returns all stored entries as a key-to-value dictionary.</summary>
    public IDictionary<string, object?> ToDictionary();
    /// <summary>Returns a read-only view of the stored entries.</summary>
    public IReadableMessageData ToReadable();
}
