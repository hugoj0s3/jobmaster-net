namespace JobMaster.Abstractions.Models;

/// <summary>
/// Fluent builder for attaching key-value metadata to a job or recurring schedule at scheduling time.
/// Create an instance via <see cref="WritableMetadata.New"/> and pass it to any scheduler method.
/// All <c>Set*</c> methods return the same instance to allow method chaining.
/// </summary>
public interface IWritableMetadata
{
    /// <summary>Stores a <see cref="string"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetStringValue(string key, string value);
    /// <summary>Stores an <see cref="int"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetIntValue(string key, int value);
    /// <summary>Stores a <see cref="long"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetLongValue(string key, long value);
    /// <summary>Stores a <see cref="char"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetCharValue(string key, char value);
    /// <summary>Stores a <see cref="bool"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetBoolValue(string key, bool value);
    /// <summary>Stores a <see cref="double"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetDoubleValue(string key, double value);
    /// <summary>Stores a <see cref="DateTime"/> value under <paramref name="key"/>, preserving its
    /// <see cref="DateTime.Kind"/> (Utc, Local, or Unspecified) -- read back via <see cref="IReadableMetadata.GetDateTimeValue"/>,
    /// which figures out the correct kind on deserialization.</summary>
    public IWritableMetadata SetDateTimeValue(string key, DateTime value);
    /// <summary>Stores a <see cref="decimal"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetDecimalValue(string key, decimal value);
    /// <summary>Stores a <see cref="short"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetShortValue(string key, short value);
    /// <summary>Stores a <see cref="byte"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetByteValue(string key, byte value);
    /// <summary>Stores a <see cref="Guid"/> value under <paramref name="key"/>.</summary>
    public IWritableMetadata SetGuidValue(string key, Guid value);

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
