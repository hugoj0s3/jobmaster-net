namespace JobMaster.Abstractions;

/// <summary>
/// Base class for attributes that bundle a <see cref="JobDefinitionConfig"/>. A concrete subclass
/// can live in a shared "contracts" assembly referenced by both a publisher (which schedules jobs via
/// <see cref="IJobMasterSchedulerAdvanced.OnceNow{TDefinition}"/> and friends, using the attribute type
/// as a generic argument) and a consumer (which applies the same attribute on its <see cref="IJobMasterHandler"/>
/// implementation), without the publisher ever needing to reference the handler's assembly.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public abstract class JobDefinitionConfigAttribute : Attribute
{
    /// <summary>The definition's identity and scheduling configuration.</summary>
    public abstract JobDefinitionConfig Config { get; }
}
