namespace JobMaster.Abstractions.Models;

/// <summary>
/// Read-only typed access to the key-value metadata attached to a job or recurring schedule.
/// Available via <see cref="JobContext.Metadata"/> and <see cref="RecurringScheduleContext.Metadata"/> inside a handler.
/// <para>
/// <c>TryGet*</c> methods return <c>null</c> when the key does not exist.
/// <c>Get*</c> methods throw when the key is missing or the type does not match.
/// </para>
/// </summary>
public interface IReadableMetadata
{
    /// <summary>Returns the <see cref="string"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public string GetStringValue(string key);
    /// <summary>Returns the <see cref="int"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public int GetIntValue(string key);
    /// <summary>Returns the <see cref="long"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public long GetLongValue(string key);
    /// <summary>Returns the <see cref="char"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public char GetCharValue(string key);
    /// <summary>Returns the <see cref="bool"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public bool GetBoolValue(string key);
    /// <summary>Returns the <see cref="double"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public double GetDoubleValue(string key);
    /// <summary>Returns the <see cref="DateTime"/> value for <paramref name="key"/>, with its original
    /// <see cref="DateTime.Kind"/> (Utc, Local, or Unspecified) restored. Throws if absent or wrong type.</summary>
    public DateTime GetDateTimeValue(string key);
    /// <summary>Returns the <see cref="decimal"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public decimal GetDecimalValue(string key);
    /// <summary>Returns the <see cref="short"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public short GetShortValue(string key);
    /// <summary>Returns the <see cref="byte"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public byte GetByteValue(string key);
    /// <summary>Returns the <see cref="Guid"/> value for <paramref name="key"/>. Throws if absent or wrong type.</summary>
    public Guid GetGuidValue(string key);

    /// <summary>Returns the <see cref="string"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public string? TryGetStringValue(string key);
    /// <summary>Returns the <see cref="int"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public int? TryGetIntValue(string key);
    /// <summary>Returns the <see cref="long"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public long? TryGetLongValue(string key);
    /// <summary>Returns the <see cref="char"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public char? TryGetCharValue(string key);
    /// <summary>Returns the <see cref="bool"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public bool? TryGetBoolValue(string key);
    /// <summary>Returns the <see cref="double"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public double? TryGetDoubleValue(string key);
    /// <summary>Returns the <see cref="DateTime"/> value for <paramref name="key"/>, with its original
    /// <see cref="DateTime.Kind"/> (Utc, Local, or Unspecified) restored, or <c>null</c> if absent.</summary>
    public DateTime? TryGetDateTimeValue(string key);
    /// <summary>Returns the <see cref="decimal"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public decimal? TryGetDecimalValue(string key);
    /// <summary>Returns the <see cref="short"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public short? TryGetShortValue(string key);
    /// <summary>Returns the <see cref="byte"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public byte? TryGetByteValue(string key);
    /// <summary>Returns the <see cref="Guid"/> value for <paramref name="key"/>, or <c>null</c> if absent.</summary>
    public Guid? TryGetGuidValue(string key);

    /// <summary>Returns all entries as a dictionary.</summary>
    public IDictionary<string, object?> ToDictionary();
}
