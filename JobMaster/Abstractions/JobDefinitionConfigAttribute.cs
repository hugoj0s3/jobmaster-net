using System.Reflection;

namespace JobMaster.Abstractions;

/// <summary>
/// Base class for attributes that bundle a <see cref="JobDefinitionConfig"/>. A concrete subclass
/// can live in a shared "contracts" assembly referenced by both a publisher (which schedules jobs via
/// <see cref="IJobMasterSchedulerAdvanced.OnceNow{TDefinition}"/> and friends, using the attribute type
/// as a generic argument) and a consumer (which applies the same attribute on its <see cref="IJobMasterHandler"/>
/// implementation), without the publisher ever needing to reference the handler's assembly.
/// <para>
/// A subclass must declare its own <c>public static JobDefinitionConfig Config { get; }</c> and implement
/// <see cref="IStaticJobDefinitionConfig"/> — <c>Config</c> is deliberately static, not per-instance: the
/// whole point of <see cref="IJobMasterSchedulerAdvanced"/>'s generic overloads is that publisher and
/// consumer agree on a shared identity via the definition's *type*, which an instance-derived config could
/// silently violate (e.g. a constructor parameter producing a different config per application site for
/// the same type). C# only supports <c>static abstract</c> interface members on interfaces implemented
/// directly by a concrete class — an abstract base class in between can't defer the obligation the way it
/// can for ordinary instance members — so <see cref="IStaticJobDefinitionConfig"/> must be declared on
/// each concrete subclass, not here.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public abstract class JobDefinitionConfigAttribute : Attribute
{
    /// <summary>
    /// Reads the static <c>Config</c> declared on <paramref name="definitionType"/> via reflection, if any.
    /// Works for any type implementing <see cref="IStaticJobDefinitionConfig"/>, not only
    /// <see cref="JobDefinitionConfigAttribute"/> subclasses. Mirrors how
    /// <see cref="StaticRecurringSchedules.IStaticRecurringSchedulesProfile"/>'s own static members are
    /// read, since discovery here only ever has a <see cref="Type"/> in hand, never a compile-time generic
    /// parameter that could use <see cref="IStaticJobDefinitionConfig"/> directly.
    /// </summary>
    internal static bool TryGetConfig(Type definitionType, out JobDefinitionConfig? config)
    {
        config = definitionType.GetProperty("Config", BindingFlags.Public | BindingFlags.Static)
            ?.GetValue(null) as JobDefinitionConfig;
        return config is not null;
    }

    /// <summary>
    /// Like <see cref="TryGetConfig"/>, but throws instead of returning <c>false</c> when
    /// <paramref name="definitionType"/> has no valid static <c>Config</c>.
    /// </summary>
    internal static JobDefinitionConfig GetConfig(Type definitionType)
    {
        if (!TryGetConfig(definitionType, out var config))
        {
            throw new InvalidOperationException(
                $"{definitionType.FullName} does not declare a `public static JobDefinitionConfig Config {{ get; }}` " +
                "member. Every IStaticJobDefinitionConfig implementation must provide one (this is enforced at " +
                "compile time when building for net8.0).");
        }

        return config!;
    }

    /// <summary>
    /// Finds the <see cref="JobDefinitionConfigAttribute"/> applied to <paramref name="handlerType"/>, if
    /// any, and reads its static <c>Config</c>. Returns <c>null</c> if no such attribute is applied.
    /// </summary>
    internal static JobDefinitionConfig? TryGetAppliedConfig(Type handlerType)
    {
        var attr = handlerType.GetCustomAttribute<JobDefinitionConfigAttribute>();
        return attr is null ? null : GetConfig(attr.GetType());
    }
}
