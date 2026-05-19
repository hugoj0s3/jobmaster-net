namespace JobMaster.Abstractions.Models;

/// <summary>
/// Fluent builder for attaching key-value metadata to a job or recurring schedule at scheduling time.
/// Create an instance via <see cref="WritableMetadata.New"/> and pass it to any scheduler method.
/// All <c>Set*</c> methods return the same instance to allow method chaining.
/// </summary>
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

    /// <summary>Returns all entries as a dictionary.</summary>
    public IDictionary<string, object?> ToDictionary();

    /// <summary>Returns a read-only view of this metadata.</summary>
    public IReadableMetadata ToReadable();
}

/// <summary>Factory for creating <see cref="IWritableMetadata"/> instances.</summary>
public static class WritableMetadata
{
    /// <summary>Creates a new empty metadata builder.</summary>
    public static IWritableMetadata New() => new Metadata();

    /// <summary>Creates a metadata builder pre-populated from an existing dictionary.</summary>
    public static IWritableMetadata FromDictionary(IDictionary<string, object?> dictionary) => new Metadata(dictionary);
}
