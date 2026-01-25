using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using JobMaster.Abstractions.Models;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Serialization;

namespace JobMaster.Sdk.Abstractions.Models.GenericRecords;

internal static class MasterGenericRecordGroupIds
{
    public const string Bucket = "Bucket";
    public const string ClusterConfiguration = "ClusterConfiguration";
    public const string AgentWorker = "AgentWorker";
    public const string Sentinel = "Sentinel";
    public const string AgentWorkerHeartbeat = "AgentWorkerHeartbeat";
    public const string Log = "Log";
    public const string JobMetadata = "JobMasterMetadata";
    public const string RecurringScheduleMetadata = "RecurringScheduleMetadata";
}

internal class GenericRecordEntry : JobMasterBaseModel
{
    public string RecordUniqueId { get; private set; } = string.Empty;
    public string EntryId { get; private set; } = string.Empty;
    public string GroupId { get; private set; } = string.Empty;
    public string? SubjectType { get; set; }
    public string? SubjectId { get; set; }
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
        string scopeType,
        string subjectId,
        T obj,
        DateTime? expiresAt = null)
    {
        return CreateImpl(clusterId, groupId, entryId, scopeType, subjectId, obj, expiresAt);
    }

    public static GenericRecordEntry Create<T>(
        string clusterId,
        string groupId,
        string entryId,
        T obj,
        DateTime? expiresAt = null)
    {
        return CreateImpl(clusterId, groupId, entryId, null, null, obj, expiresAt);
    }
    
    public static GenericRecordEntry Create<T>(
        string clusterId,
        string groupId,
        Guid entryId,
        T obj,
        DateTime? expiresAt = null)
    {
        return CreateImpl(clusterId, groupId, entryId, null, null, obj, expiresAt);
    }
    
    public static GenericRecordEntry Create<T>(
        string clusterId,
        string groupId,
        Guid entryId,
        string subjectType,
        string subjectId,
        T obj,
        DateTime? expiresAt = null)
    {
        return CreateImpl(clusterId, groupId, entryId,subjectType, subjectId, obj, expiresAt);
    }
    
    public IReadableMetadata ToReadable() => new Metadata(this.DictionaryShallowCopy());
    
    public static GenericRecordEntry FromWritableMetadata(
        string clusterId,
        string groupId,
        string entryId,
        IWritableMetadata metadata,
        DateTime? expiresAt = null)
    {
        EnsureIdsIsValid(clusterId, groupId, entryId, null);
        
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
    
    public static GenericRecordEntry FromWritableMetadata(  
        string clusterId,
        string groupId,
        Guid entryId,
        string subjectType,
        string subjectId,
        IWritableMetadata metadata,
        DateTime? expiresAt = null)
    {
        EnsureIdsIsValid(clusterId, groupId, entryId.ToString("N"), subjectId);
        var dict = metadata.ToDictionary();
        return new GenericRecordEntry(clusterId)
        {
            RecordUniqueId = UniqueId(clusterId, groupId, entryId),
            GroupId = groupId,
            EntryId = entryId.ToString("N"),
            SubjectType = subjectType,
            SubjectId = subjectId,
            CreatedAt = DateTime.UtcNow,
            ExpiresAt = expiresAt,
            Values = dict,
        };
    }

    private static GenericRecordEntry CreateImpl<T>(
        string clusterId,
        string groupId,
        Guid entryId,
        string? subjectType,
        string? subjectId,
        T obj,
        DateTime? expiresAt = null)
    {
        var entryIdStr = entryId.ToString("N");
        return CreateImpl(clusterId, groupId, entryIdStr, subjectType, subjectId, obj, expiresAt);
    }

    private static GenericRecordEntry CreateImpl<T>(
        string clusterId,
        string groupId,
        string entryId,
        string? subjectType,
        string? subjectId,
        T obj,
        DateTime? expiresAt = null) 
    {
        EnsureIdsIsValid(clusterId, groupId, entryId, subjectId);

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
            SubjectType = subjectType,
            SubjectId = subjectId,
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

        if (t == typeof(Guid))     return value is Guid g ? g : Guid.Parse(value.ToString()!);
        
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
            if (stored is DateTime dt) return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
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
    
    private static void EnsureIdsIsValid(string clusterId, string groupId, string entryId, string? subjectId)
    {
        if (!JobMasterStringUtils.IsValidForId(clusterId) || !JobMasterStringUtils.IsValidForId(groupId) || !JobMasterStringUtils.IsValidForId(entryId))
        {
            throw new ArgumentException($"Invalid ID format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. clusterId: {clusterId}, groupId: {groupId}, entryId: {entryId}");
        }

        if (subjectId != null && !JobMasterStringUtils.IsValidForId(subjectId))
        {
            throw new ArgumentException($"Invalid subject ID format. Only letters, numbers, underscore (_), hyphen (-), and dot (.) are allowed. subjectId: {subjectId}");
        }
    }
    
    private IDictionary<string, object?> DictionaryShallowCopy() => new Dictionary<string, object?>(Values); 
}
