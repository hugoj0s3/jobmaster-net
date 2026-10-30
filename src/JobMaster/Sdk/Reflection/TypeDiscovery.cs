using System.Collections.Concurrent;
using System.Reflection;
using JobMaster.Abstractions.StaticRecurringSchedules;

namespace JobMaster.Sdk.Reflection;

/// <summary>
/// The types of every loaded assembly, for the AppDomain-wide scans (startup validation, IoC registration, handler
/// lookup). Each assembly's types are read once and cached: an assembly's types never change once it's loaded.
/// The assembly list itself is re-read on every call, so an assembly loaded later is picked up by the next call.
/// <para>
/// An assembly whose types can't all be loaded (<see cref="ReflectionTypeLoadException"/>, e.g. a missing dependency)
/// contributes the types that did load, instead of failing the scan.
/// </para>
/// </summary>
internal static class TypeDiscovery
{
    private static readonly ConcurrentDictionary<Assembly, Type[]> TypesByAssembly = new();
    private static readonly ConcurrentDictionary<(Assembly Assembly, Type BaseType), Type[]> ConcreteTypesByAssembly = new();
    private static readonly ConcurrentDictionary<Assembly, byte> AddedAssemblies = new();

    /// <summary>Every type of every loaded assembly.</summary>
    public static IEnumerable<Type> AllTypes() => AllTypes(GetLoadedAssemblies());

    /// <summary>Every non-abstract, non-interface type of the loaded assemblies that is assignable to <paramref name="baseType"/>.</summary>
    public static IEnumerable<Type> GetCachedConcreteTypesOf(this Type baseType) => baseType.GetCachedConcreteTypesOf(GetLoadedAssemblies());

    /// <summary>
    /// Every non-abstract static recurring schedule profile type. Matched by the interface name, like the bootstrap
    /// always has, rather than <see cref="Type.IsAssignableFrom"/>.
    /// </summary>
    public static IEnumerable<Type> StaticRecurringSchedulesProfileTypes() =>
        AllTypes().Where(t => !t.IsAbstract && t.GetInterfaces().Any(i => i.Name == nameof(IStaticRecurringSchedulesProfile)));

    /// <summary>
    /// The assemblies the scans look at: every non-dynamic loaded one, plus the ones added through
    /// <c>JobMasterAssemblyDiscovery()</c>.
    /// </summary>
    public static IEnumerable<Assembly> GetLoadedAssemblies() =>
        AppDomain.CurrentDomain.GetAssemblies().Where(a => !a.IsDynamic).Union(AddedAssemblies.Keys);

    internal static void AddAssembly(Assembly assembly) => AddedAssemblies.TryAdd(assembly, 0);

    /// <summary>The (cached) types of <paramref name="assembly"/>.</summary>
    public static Type[] TypesOf(Assembly assembly) => TypesByAssembly.GetOrAdd(assembly, GetTypes);

    internal static IEnumerable<Type> AllTypes(IEnumerable<Assembly> assemblies) => assemblies.SelectMany(TypesOf);

    internal static IEnumerable<Type> GetCachedConcreteTypesOf(this Type baseType, IEnumerable<Assembly> assemblies)
    {
        return assemblies.SelectMany(a => ConcreteTypesByAssembly.GetOrAdd((a, baseType), GetConcreteTypes));
    }

    private static Type[] GetConcreteTypes((Assembly Assembly, Type BaseType) key)
    {
        return TypesOf(key.Assembly)
            .Where(t => !t.IsAbstract && !t.IsInterface && key.BaseType.IsAssignableFrom(t))
            .ToArray();
    }

    private static Type[] GetTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(t => t != null).ToArray()!;
        }
        catch
        {
            // Anything else (e.g. a provider that can't enumerate its types): skip the assembly, don't fail the scan.
            return Array.Empty<Type>();
        }
    }
}
