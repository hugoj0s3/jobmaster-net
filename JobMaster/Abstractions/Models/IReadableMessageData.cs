namespace JobMaster.Abstractions.Models;

/// <summary>
/// Read-only typed access to the message data payload attached to a job.
/// Available via <see cref="JobContext.MsgData"/> inside a handler.
/// <para>
/// <c>TryGet*</c> methods return <c>null</c> when the key does not exist.
/// <c>Get*</c> methods throw when the key is missing or the type does not match.
/// </para>
/// </summary>
public interface IReadableMessageData
{
    public string? TryGetStringValue(string key);
    public int? TryGetIntValue(string key);
    public long? TryGetLongValue(string key);
    public char? TryGetCharValue(string key);
    public bool? TryGetBoolValue(string key);
    public double? TryGetDoubleValue(string key);
    public DateTime? TryGetDateTimeValue(string key);
    public TEnum? TryGetEnumValue<TEnum>(string key) where TEnum : struct, Enum;
    public byte[]? TryGetByteArrayValue(string key);
    public decimal? TryGetDecimalValue(string key);
    public short? TryGetShortValue(string key);
    public byte? TryGetByteValue(string key);
    public Guid? TryGetGuidValue(string key);
    public T? TryGetJson<T>(string key) where T : new();

    public string GetStringValue(string key);
    public int GetIntValue(string key);
    public long GetLongValue(string key);
    public char GetCharValue(string key);
    public bool GetBoolValue(string key);
    public double GetDoubleValue(string key);
    public DateTime GetDateTimeValue(string key);
    public TEnum GetEnumValue<TEnum>(string key) where TEnum : struct, Enum;
    public byte[] GetByteArrayValue(string key);
    public decimal GetDecimalValue(string key);
    public short GetShortValue(string key);
    public byte GetByteValue(string key);
    public Guid GetGuidValue(string key);
    public T GetJson<T>(string key) where T : new();

    /// <summary>Returns all entries as a dictionary.</summary>
    public IDictionary<string, object?> ToDictionary();
}
