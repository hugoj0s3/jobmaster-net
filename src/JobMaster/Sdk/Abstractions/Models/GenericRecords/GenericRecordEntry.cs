using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Models.GenericRecords;

internal static class MasterGenericRecordGroupIds
{
    public const string ClusterConfiguration = "ClusterConfiguration";

    public const string Host = "Host";
    public const string AgentWorker = "AgentWorker";
    public const string Bucket = "Bucket";
    public const string AgentConnection = "AgentConnection"; // Don't store connection string or any sensitive information.
    
    
    public const string Sentinel = "Sentinel";
    public const string AgentWorkerHeartbeat = "AgentWorkerHeartbeat";
    public const string AgentConnectionHeartbeat = "AgentConnectionHeartbeat";
    public const string HostHeartbeat = "HostHeartbeat";
    
    public const string JobMetadata = "JobMasterMetadata";
    public const string RecurringScheduleMetadata = "RecurringScheduleMetadata";
}

internal class GenericRecordEntry : JobMasterBaseModel
{
    public string RecordUniqueId { get; private set; } = string.Empty;
    public string EntryId { get; private set; } = string.Empty;
    public string GroupId { get; private set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? ExpiresAt { get; set; }
    public IDictionary<string, object?> Values { get; set; } = new Dictionary<string, object?>();
    
    private GenericRecordEntry(string clusterId) : base(clusterId)
    {
    }
    
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropsCache =
        new();

    private static PropertyInfo[] GetUsableProps(Type t) =>
        PropsCache.GetOrAdd(t, tp =>
            tp.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(p => p.CanRead && p.CanWrite && p.GetIndexParameters().Length == 0)
                .ToArray()
        );
    
    public static GenericRecordEntry Create<T>(
        string clusterId,
        string groupId,
        string entryId,
        T obj,
        DateTime? expiresAt = null)
    {
        return CreateImpl(clusterId, groupId, entryId, obj, expiresAt);
    }

    public static GenericRecordEntry Create<T>(
        string clusterId,
        string groupId,
        Guid entryId,
        T obj,
        DateTime? expiresAt = null)
    {
        return CreateImpl(clusterId, groupId, entryId, obj, expiresAt);
    }
    
    public IReadableMetadata ToReadable() => new Metadata(this.DictionaryShallowCopy());
    
    public static GenericRecordEntry FromWritableMetadata(
        string clusterId,
        string groupId,
        string entryId,
        IWritableMetadata metadata,
        DateTime? expiresAt = null)
    {
        EnsureIdsIsValid(clusterId, groupId, entryId);

        var dict = metadata.ToDictionary();
        return new GenericRecordEntry(clusterId)
        {
            RecordUniqueId = UniqueId(clusterId, groupId, entryId),
            GroupId = groupId,
            EntryId = entryId,
            CreatedAt = DateTime.UtcNow,
            ExpiresAt = expiresAt,
            Values = dict,
        };
    }

    private static GenericRecordEntry CreateImpl<T>(
        string clusterId,
        string groupId,
        Guid entryId,
        T obj,
        DateTime? expiresAt = null)
    {
        var entryIdStr = entryId.ToString("N");
        return CreateImpl(clusterId, groupId, entryIdStr, obj, expiresAt);
    }

    private static GenericRecordEntry CreateImpl<T>(
        string clusterId,
        string groupId,
        string entryId,
        T obj,
        DateTime? expiresAt = null)
    {
        EnsureIdsIsValid(clusterId, groupId, entryId);

        var uniqueId = UniqueId(clusterId, groupId, entryId);

        var props = GetUsableProps(typeof(T));

        IDictionary<string, object?> values = new Dictionary<string, object?>(StringComparer.Ordinal);

        foreach (var p in props)
        {
            var key = p.Name;
            var val = p.GetValue(obj);

            values[key] = ToStorageObject(val, p.PropertyType);
        }

        return new GenericRecordEntry(clusterId)
        {
            RecordUniqueId = uniqueId,
            GroupId = groupId,
            EntryId = entryId,
            ExpiresAt = expiresAt,
            CreatedAt = DateTime.UtcNow,
            Values = values
        };
    }

    public static string UniqueId(string clusterId, string groupId, string entryId)
    {
        return $"{clusterId}:{groupId}:{entryId}";
    }
    
    public static string UniqueId(string clusterId, string groupId, Guid entryId)
    {
        return $"{clusterId}:{groupId}:{entryId:N}";
    }

    public T ToObject<T>()
    {
        var targetType = typeof(T);
        var obj = Activator.CreateInstance(targetType, nonPublic: true)!;

        var props = GetUsableProps(targetType);

        foreach (var p in props)
        {
            if (!Values.TryGetValue(p.Name, out var stored)) continue;
            
            if (!p.CanWrite) continue;
            
            var restored = FromStorageObject(stored, p.PropertyType);
            p.SetValue(obj, restored);
        }

        return (T)obj;
    }

     private static object? ToStorageObject(object? value, Type declaredType)
    {
        if (value is null) return null;

        var t = Nullable.GetUnderlyingType(declaredType) ?? declaredType;

        if (t.IsEnum)
        {
            var ut = Enum.GetUnderlyingType(t);
            return Convert.ChangeType(value, ut, CultureInfo.InvariantCulture);
        }

        if (t == typeof(string))   return (string)value;
        if (t == typeof(bool))     return Convert.ToBoolean(value, CultureInfo.InvariantCulture);

        if (t == typeof(byte))     return Convert.ToByte(value, CultureInfo.InvariantCulture);
        if (t == typeof(sbyte))    return Convert.ToSByte(value, CultureInfo.InvariantCulture);
        if (t == typeof(short))    return Convert.ToInt16(value, CultureInfo.InvariantCulture);
        if (t == typeof(ushort))   return Convert.ToUInt16(value, CultureInfo.InvariantCulture);
        if (t == typeof(int))      return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        if (t == typeof(uint))     return Convert.ToUInt32(value, CultureInfo.InvariantCulture);
        if (t == typeof(long))     return Convert.ToInt64(value, CultureInfo.InvariantCulture);
        if (t == typeof(ulong))    return Convert.ToUInt64(value, CultureInfo.InvariantCulture);

        if (t == typeof(decimal))  return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        if (t == typeof(double))   return Convert.ToDouble(value, CultureInfo.InvariantCulture);
        if (t == typeof(float))    return Convert.ToSingle(value, CultureInfo.InvariantCulture);

        if (t == typeof(DateTime))
            return Convert.ToDateTime(value, CultureInfo.InvariantCulture);

        if (t == typeof(DateTimeOffset))
        {
            var dto = value is DateTimeOffset dtoIn
                ? dtoIn
                : DateTimeOffset.Parse(value.ToString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            
            return dto.ToUniversalTime();
        }

        if (t == typeof(Guid))
        {
            return value is Guid g ? g : Guid.Parse(value.ToString()!);
        }    
            
        
#if NET6_0_OR_GREATER
        // Optional: support DateOnly/TimeOnly if you use them
        if (t == typeof(DateOnly)) return value;
        if (t == typeof(TimeOnly)) return value;
#endif

        if (t == typeof(JobMasterConfigDictionary))
        {
            var cfg = (JobMasterConfigDictionary)value;
            var dict = cfg.GetFullDictionary();
            
            return dict;
        }
        
        if (t == typeof(AgentConnectionId))
        {
            return value is AgentConnectionId connId ? connId.IdValue : null;
        }
        
        if (t == typeof(HostId))
        {
            return value is HostId hostId ? hostId.IdValue + "||" + hostId.HostDisplayName : null;
        }
        
        // Complex types → JSON string using internal serializer/options
        return InternalJobMasterSerializer.Serialize(value);
    }

    private static object? FromStorageObject(object? stored, Type declaredType)
    {
        if (stored is null) return null;
        var t = Nullable.GetUnderlyingType(declaredType) ?? declaredType;

        if (t.IsEnum)
        {
            // Accept either numeric or string names
            if (stored is string s && !string.IsNullOrWhiteSpace(s) && !char.IsDigit(s[0]))
                return Enum.Parse(t, s, ignoreCase: true);

            var ut = Enum.GetUnderlyingType(t);
            var numeric = Convert.ChangeType(stored, ut, CultureInfo.InvariantCulture);
            return Enum.ToObject(t, numeric!);
        }

        if (t == typeof(string))   return stored.ToString();
        if (t == typeof(bool))     return Convert.ToBoolean(stored, CultureInfo.InvariantCulture);

        if (t == typeof(byte))     return Convert.ToByte(stored, CultureInfo.InvariantCulture);
        if (t == typeof(sbyte))    return Convert.ToSByte(stored, CultureInfo.InvariantCulture);
        if (t == typeof(short))    return Convert.ToInt16(stored, CultureInfo.InvariantCulture);
        if (t == typeof(ushort))   return Convert.ToUInt16(stored, CultureInfo.InvariantCulture);
        if (t == typeof(int))      return Convert.ToInt32(stored, CultureInfo.InvariantCulture);
        if (t == typeof(uint))     return Convert.ToUInt32(stored, CultureInfo.InvariantCulture);
        if (t == typeof(long))     return Convert.ToInt64(stored, CultureInfo.InvariantCulture);
        if (t == typeof(ulong))    return Convert.ToUInt64(stored, CultureInfo.InvariantCulture);

        if (t == typeof(decimal))  return Convert.ToDecimal(stored, CultureInfo.InvariantCulture);
        if (t == typeof(double))   return Convert.ToDouble(stored, CultureInfo.InvariantCulture);
        if (t == typeof(float))    return Convert.ToSingle(stored, CultureInfo.InvariantCulture);
        
        if (t == typeof(DateTime))
        {
            if (stored is DateTime dt) return dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
            var s = stored.ToString()!;
            return DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind).ToUniversalTime();
        }

        if (t == typeof(DateTimeOffset))
        {
            if (stored is DateTimeOffset dto) return dto.ToUniversalTime();
            var s = stored.ToString()!;
            return DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind).ToUniversalTime();
        }


        if (t == typeof(Guid))
            return stored is Guid g ? g : Guid.Parse(stored.ToString()!);
        
#if NET6_0_OR_GREATER
        if (t == typeof(DateOnly))
        {
            if (stored is DateOnly dO) return dO;
            if (stored is DateTime d)  return DateOnly.FromDateTime(d);
            return DateOnly.Parse(stored.ToString()!, CultureInfo.InvariantCulture);
        }

        if (t == typeof(TimeOnly))
        {
            if (stored is TimeOnly tO) return tO;
            if (stored is DateTime d)  return TimeOnly.FromDateTime(d);
            return TimeOnly.Parse(stored.ToString()!, CultureInfo.InvariantCulture);
        }
#endif
        
        if (t == typeof(JobMasterConfigDictionary))
        {
            var dictionary = InternalJobMasterSerializer.Deserialize<IDictionary<string, object>>(stored.ToString()!);
            var config = new JobMasterConfigDictionary(dictionary);
            return config;
        }
        
        if (t == typeof(AgentConnectionId))
        {
            try 
            {
                return new AgentConnectionId(stored.ToString()!);
            }
            catch (ArgumentException)
            {
               return null;
            }
        }

        if (t == typeof(HostId))
        {
            try
            {
                var value = stored?.ToString();
                if (string.IsNullOrWhiteSpace(value))
                    return null;

                // split into at most 2 parts in case display name contains '||' defensively
                var parts = value!.Split(["||"], 2, StringSplitOptions.None);
                if (parts.Length != 2)
                    return null;

                var id = parts[0];
                var displayName = parts[1];

                // Use Recover helper so you keep the same rules everywhere
                return HostId.Recover(displayName, id);
            }
            catch (ArgumentException)
            {
                return null;
            }
        }

        // try JSON for complex types using internal serializer
        if (stored is string json)
        {
            try { return InternalJobMasterSerializer.TryDeserialize(json, t); }
            catch { /* ignore */ }
        }

        // last resort
        try { return Convert.ChangeType(stored, t, CultureInfo.InvariantCulture); }
        catch { return null; }
    }
    
    private static void EnsureIdsIsValid(string clusterId, string groupId, string entryId)
    {
        if (!JobMasterStringUtils.IsValidForId(clusterId) || !JobMasterStringUtils.IsValidForId(groupId) || !JobMasterStringUtils.IsValidForId(entryId))
        {
            throw new ArgumentException($"Invalid ID format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. clusterId: {clusterId}, groupId: {groupId}, entryId: {entryId}");
        }
    }
    
    private IDictionary<string, object?> DictionaryShallowCopy() => new Dictionary<string, object?>(Values);

    public static T? DeepClone<T>(T? value)
    {
        if (value is null) return default;

        var runtimeType = value.GetType();

        if (runtimeType.IsPrimitive || runtimeType.IsEnum) return value;
        if (value is string or DateTime or DateTimeOffset or TimeSpan or decimal or Guid) return value;

        if (runtimeType.IsArray)
        {
            var elementType = runtimeType.GetElementType()!;
            var src = (Array)(object)value;
            var dst = Array.CreateInstance(elementType, src.Length);
            for (var i = 0; i < src.Length; i++)
            {
                dst.SetValue(CloneViaStorage(src.GetValue(i), elementType), i);
            }
            return (T)(object)dst;
        }

        if (runtimeType.IsGenericType && runtimeType.GetGenericTypeDefinition() == typeof(List<>))
        {
            var elementType = runtimeType.GetGenericArguments()[0];
            var src = (System.Collections.IList)value;
            var dst = (System.Collections.IList)Activator.CreateInstance(runtimeType, src.Count)!;
            foreach (var item in src)
            {
                dst.Add(CloneViaStorage(item, elementType));
            }
            return (T)dst;
        }

        return (T)CloneViaStorage(value, runtimeType)!;
    }

    private static object? CloneViaStorage(object? value, Type declaredType)
    {
        if (value is null) return null;

        var t = Nullable.GetUnderlyingType(declaredType) ?? declaredType;
        if (t.IsPrimitive || t.IsEnum) return value;
        if (value is string or DateTime or DateTimeOffset or TimeSpan or decimal or Guid) return value;

        var actualType = value.GetType();
        var copy = Activator.CreateInstance(actualType, nonPublic: true)
                   ?? throw new InvalidOperationException(
                       $"DeepCloneViaGenericEntry cannot clone {actualType.FullName}: no accessible parameterless constructor.");

        var props = GetUsableProps(actualType);
        foreach (var p in props)
        {
            var srcVal = p.GetValue(value);
            var stored = ToStorageObject(srcVal, p.PropertyType);

            // Mirror the storage layer's JSON step. Some FromStorageObject branches
            // (e.g. JobMasterConfigDictionary, complex-type fallback) expect a JSON
            // string because in production the GenericRecordEntry.Values dict is
            // serialized to JSON when persisted. ToStorageObject returns scalars
            // as-is, and non-scalars as dictionaries or already-serialized JSON.
            // Re-serialize anything that isn't a scalar so FromStorageObject sees
            // the same shape it would after a real storage round-trip.
            if (stored is not null && !IsScalarStorageValue(stored))
            {
                stored = InternalJobMasterSerializer.Serialize(stored);
            }

            var restored = FromStorageObject(stored, p.PropertyType);
            p.SetValue(copy, restored);
        }

        return copy;
    }

    private static bool IsScalarStorageValue(object value)
    {
        var t = value.GetType();
        if (t.IsPrimitive || t.IsEnum) return true;
        return value is string
            or DateTime
            or DateTimeOffset
            or TimeSpan
            or decimal
            or Guid
#if NET6_0_OR_GREATER
            or DateOnly
            or TimeOnly
#endif
            ;
    }
}

internal static class GenericRecordEntryExtensions
{
    public static T? DeepCloneViaGenericEntry<T>(this T? value)
        => GenericRecordEntry.DeepClone(value);
}
