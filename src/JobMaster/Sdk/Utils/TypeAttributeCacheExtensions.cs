using System.Collections.Concurrent;
using System.Reflection;

namespace JobMaster.Sdk.Utils;

/// <summary>
/// Cached counterparts of <see cref="CustomAttributeExtensions.GetCustomAttribute{T}(MemberInfo)"/> /
/// <see cref="MemberInfo.GetCustomAttributes(bool)"/> for the per-job hot path (resolving handler settings on every
/// schedule call). Reflection re-reads metadata and allocates new attribute instances on every call; attributes are
/// fixed at compile time, so they're resolved once per (type, attribute type, inherit) and reused.
/// <para>
/// Cached instances are shared between callers, so they must never be mutated after reading (some, like
/// <c>JobMasterMetadataAttribute</c>, have setters only to support named attribute arguments). Use
/// plain reflection for assembly-wide scans (startup validation, discovery), which would otherwise fill the cache
/// with every type in the AppDomain.
/// </para>
/// </summary>
internal static class TypeAttributeCacheExtensions
{
    // Keyed by the requested attribute type too, and filled with the type-filtered reflection call: that only
    // instantiates matching attributes, exactly like the uncached call. Instantiating every attribute on the type
    // instead would surface unrelated attribute-constructor exceptions (e.g. an out-of-range
    // [JobMasterMaxNumberOfRetries]) -- GetJobHandlerTypeFromId scans every handler type, so one bad handler would
    // break handler lookup for all jobs.
    private static readonly ConcurrentDictionary<(Type Type, Type AttributeType, bool Inherit), Attribute[]> Cache = new();

    /// <summary>
    /// Cached lookup of the first <typeparamref name="T"/> on <paramref name="type"/>, or <c>null</c>. Unlike
    /// <see cref="CustomAttributeExtensions.GetCustomAttribute{T}(MemberInfo, bool)"/> it doesn't throw on multiple
    /// matches: this runs on the per-job path (and inside scans over every handler type), where one misconfigured
    /// type would otherwise break unrelated jobs. Rules forbidding multiples belong in startup validation.
    /// </summary>
    internal static T? GetCachedAttribute<T>(this Type type, bool inherit = true) where T : Attribute
    {
        var attributes = Get(type, typeof(T), inherit);
        return attributes.Length == 0 ? null : (T)attributes[0];
    }

    /// <summary>Cached <see cref="CustomAttributeExtensions.GetCustomAttributes{T}(MemberInfo, bool)"/>.</summary>
    internal static IEnumerable<T> GetCachedAttributes<T>(this Type type, bool inherit = true) where T : Attribute
    {
        return Get(type, typeof(T), inherit).Cast<T>();
    }

    private static Attribute[] Get(Type type, Type attributeType, bool inherit)
    {
        return Cache.GetOrAdd((type, attributeType, inherit), static key => Attribute.GetCustomAttributes(key.Type, key.AttributeType, key.Inherit));
    }
}
