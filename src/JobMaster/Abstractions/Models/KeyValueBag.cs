using JobMaster.Abstractions.Serialization;
using JobMaster.Sdk.Utils;

namespace JobMaster.Abstractions.Models;

internal class KeyValueBag
{
    private readonly IDictionary<string, object?> values = JobMasterDictionaryUtils.NewIgnoreCase();

    public KeyValueBag()
    {
    }

    // Copied, not wrapped: the given dictionary (e.g. deserialized from storage) may be case-sensitive.
    public KeyValueBag(IDictionary<string, object?> values)
    {
        this.values = values.ToCaseInsensitive();
    }

    // ========= Write API =========
    public void SetStringValue(string key, string value) => Set(key, value);
    public void SetIntValue(string key, int value) => Set(key, value);
    public void SetLongValue(string key, long value) => Set(key, value);
    public void SetCharValue(string key, char value) => Set(key, value);
    public void SetBoolValue(string key, bool value) => Set(key, value);
    public void SetDoubleValue(string key, double value) => Set(key, value);
    public void SetDateTimeValue(string key, DateTime value) => Set(key, value);
    public void SetEnumValue<TEnum>(string key, TEnum value) where TEnum : struct, Enum => Set(key, value);
    public void SetByteArrayValue(string key, byte[] value) => Set(key, value);
    public void SetDecimalValue(string key, decimal value) => Set(key, value);
    public void SetShortValue(string key, short value) => Set(key, value);
    public void SetByteValue(string key, byte value) => Set(key, value);
    public void SetGuidValue(string key, Guid value) => Set(key, value);

    public void SetJson<T>(string key, T? value) where T : new()
    {
        if (value is null)
        {
            Set(key, null);
            return;
        }

        var json = JobMasterMessageJson.Serialize<T>(value);
        SetStringValue(key, json);
    }
    
    // ========= Read API (TryGet*) =========
    public string? TryGetStringValue(string key)
    {
        if (!values.TryGetValue(key, out var v) || v is null) return null;
        if (v is string s) return s;
        return v.ToString();
    }

    public int? TryGetIntValue(string key) => TryConvertNumber<int>(key, Convert.ToInt32);
    public long? TryGetLongValue(string key) => TryConvertNumber<long>(key, Convert.ToInt64);
    public short? TryGetShortValue(string key) => TryConvertNumber<short>(key, Convert.ToInt16);
    public byte? TryGetByteValue(string key) => TryConvertNumber<byte>(key, Convert.ToByte);
    public double? TryGetDoubleValue(string key) => TryConvertNumber<double>(key, Convert.ToDouble);
    public decimal? TryGetDecimalValue(string key) => TryConvertNumber<decimal>(key, Convert.ToDecimal);

    public bool? TryGetBoolValue(string key)
    {
        if (!values.TryGetValue(key, out var v) || v is null) return null;
        if (v is bool b) return b;
        if (v is string s && bool.TryParse(s, out var pb)) return pb;
        try
        {
            return Convert.ToInt32(v) != 0;
        }
        catch
        {
            return null;
        }
    }

    public char? TryGetCharValue(string key)
    {
        if (!values.TryGetValue(key, out var v) || v is null) return null;
        if (v is char c) return c;
        if (v is string s && s.Length > 0) return s[0];
        return null;
    }

    public DateTime? TryGetDateTimeValue(string key)
    {
        if (!values.TryGetValue(key, out var v) || v is null) return null;
        if (v is DateTime dt) return dt;
        if (v is string s && DateTime.TryParse(s, out var parsed)) return parsed;
        return null;
    }

    public Guid? TryGetGuidValue(string key)
    {
        if (!values.TryGetValue(key, out var v) || v is null) return null;
        if (v is Guid g) return g;
        if (v is string s && Guid.TryParse(s, out var parsed)) return parsed;
        return null;
    }

    public byte[]? TryGetByteArrayValue(string key)
    {
        if (!values.TryGetValue(key, out var v) || v is null) return null;
        if (v is byte[] bytes) return bytes;

        // Persisted MsgData comes back with byte[] as the base64 string System.Text.Json wrote.
        if (v is string s)
        {
            try { return Convert.FromBase64String(s); }
            catch (FormatException) { return null; }
        }

        return null;
    }

    public TEnum? TryGetEnumValue<TEnum>(string key, bool ignoreInvalidValues = false) where TEnum : struct, Enum
    {
        if (!values.TryGetValue(key, out var v) || v is null) return null;

        var parsed = ParseEnum<TEnum>(v);
        if (parsed.HasValue && IsDefinedEnumValue(parsed.Value)) return parsed;

        if (ignoreInvalidValues) return null;
        
        throw new InvalidOperationException($"Key '{key}' does not hold a valid {typeof(TEnum).Name} value.");
    }

    public T? TryGetJson<T>(string key, bool ignoreDeserializationErrors = false) where T : new()
    {
        if (!values.TryGetValue(key, out var v) || v is null) return default;

        try
        {
            if (v is string s)
                return JobMasterMessageJson.Deserialize<T>(s);
        }
        catch (Exception ex)
        {
            if (ignoreDeserializationErrors) return default;

            // Inner exception not attached: some serializers echo the offending payload in their message.
            throw new InvalidOperationException(
                $"Key '{key}' could not be deserialized as {typeof(T).Name} ({ex.GetType().Name}).");
        }

        if (ignoreDeserializationErrors) return default;
        throw new InvalidOperationException($"Key '{key}' does not hold a JSON string and cannot be deserialized as {typeof(T).Name}.");
    }

    // ========= Read API (Get*) =========
    public string GetStringValue(string key) => Require(TryGetStringValue(key), key, nameof(String));
    public int GetIntValue(string key) => Require(TryGetIntValue(key), key, nameof(Int32));
    public long GetLongValue(string key) => Require(TryGetLongValue(key), key, nameof(Int64));
    public short GetShortValue(string key) => Require(TryGetShortValue(key), key, nameof(Int16));
    public byte GetByteValue(string key) => Require(TryGetByteValue(key), key, nameof(Byte));
    public double GetDoubleValue(string key) => Require(TryGetDoubleValue(key), key, nameof(Double));
    public decimal GetDecimalValue(string key) => Require(TryGetDecimalValue(key), key, nameof(Decimal));
    public bool GetBoolValue(string key) => Require(TryGetBoolValue(key), key, nameof(Boolean));
    public char GetCharValue(string key) => Require(TryGetCharValue(key), key, nameof(Char));
    public DateTime GetDateTimeValue(string key) => Require(TryGetDateTimeValue(key), key, nameof(DateTime));
    public Guid GetGuidValue(string key) => Require(TryGetGuidValue(key), key, nameof(Guid));
    public byte[] GetByteArrayValue(string key) => Require(TryGetByteArrayValue(key), key, "Byte[]");
    public TEnum GetEnumValue<TEnum>(string key) where TEnum : struct, Enum => Require(TryGetEnumValue<TEnum>(key), key, typeof(TEnum).Name);
    public T GetJson<T>(string key) where T : new()
    {
        var value = TryGetJson<T>(key);
        if (value is null) throw new KeyNotFoundException($"Key '{key}' not found.");
        return value;
    }

    public IDictionary<string, object?> ToDictionary()
    {
        return values.ToCaseInsensitive();
    }

    private void Set(string key, object? value)
    {
        values[key] = value;
    }

    // ========= Helpers =========
    private static TOut Require<TOut>(TOut? value, string key, string target)
        where TOut : struct
    {
        if (value is null) throw new KeyNotFoundException($"Key '{key}' not found or not a valid {target}.");
        return value.Value;
    }

    private static TOut Require<TOut>(TOut? value, string key, string target)
        where TOut : class
    {
        if (value is null) throw new KeyNotFoundException($"Key '{key}' not found or not a valid {target}.");
        return value;
    }

    private static TEnum? ParseEnum<TEnum>(object v) where TEnum : struct, Enum
    {
        if (v is TEnum te) return te;

        if (v is string s)
        {
            // Enum.TryParse ORs "A, B" together even on a non-[Flags] enum.
            if (s.IndexOf(',') >= 0 && !IsFlagsEnum(typeof(TEnum))) return null;
            return Enum.TryParse<TEnum>(s, true, out var parsed) ? parsed : null;
        }

        try
        {
            return (TEnum)Enum.ToObject(typeof(TEnum), Convert.ToInt64(v));
        }
        catch
        {
            return null;
        }
    }

    private static bool IsDefinedEnumValue<TEnum>(TEnum value) where TEnum : struct, Enum
    {
        var type = typeof(TEnum);
        if (!IsFlagsEnum(type)) return Enum.IsDefined(type, value);

        // [Flags]: every set bit must belong to a defined member.
        ulong definedBits = 0;
        foreach (var member in Enum.GetValues(type))
            definedBits |= ToEnumBits(member);

        return (ToEnumBits(value) & ~definedBits) == 0;
    }

    private static bool IsFlagsEnum(Type enumType) => enumType.IsDefined(typeof(FlagsAttribute), false);

    private static ulong ToEnumBits(object enumValue)
    {
        return Type.GetTypeCode(Enum.GetUnderlyingType(enumValue.GetType())) == TypeCode.UInt64
            ? Convert.ToUInt64(enumValue)
            : unchecked((ulong)Convert.ToInt64(enumValue));
    }

    private T? TryConvertNumber<T>(string key, Func<object, T> projector) where T : struct
    {
        if (!values.TryGetValue(key, out var v) || v is null) return default;
        try
        {
            if (v is T direct) return direct;
            if (v is string s)
            {
                object boxed = typeof(T) switch
                {
                    var t when t == typeof(int) && int.TryParse(s, out var i) => i,
                    var t when t == typeof(long) && long.TryParse(s, out var l) => l,
                    var t when t == typeof(short) && short.TryParse(s, out var h) => h,
                    var t when t == typeof(byte) && byte.TryParse(s, out var b) => b,
                    var t when t == typeof(double) && double.TryParse(s, out var d) => d,
                    var t when t == typeof(decimal) && decimal.TryParse(s, out var m) => m,
                    _ => s
                };
                return projector(boxed);
            }

            return projector(v);
        }
        catch
        {
            return default;
        }
    }
}
