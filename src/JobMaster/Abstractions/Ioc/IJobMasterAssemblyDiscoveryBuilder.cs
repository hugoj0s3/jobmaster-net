using System.Reflection;

namespace JobMaster.Abstractions.Ioc;

/// <summary>
/// Adds assemblies for JobMaster to discover handlers, static recurring schedule profiles and job definitions in.
/// <para>
/// JobMaster finds these types in the assemblies the application has loaded. .NET loads an assembly only when code
/// uses one of its types, so a library your code never references directly (for example, a worker that only runs the
/// handlers of a <c>Billing.Handlers</c> library) is not loaded and its types are not found. Add such libraries here.
/// </para>
/// Can be called before or after <c>AddJobMasterCluster</c>, any number of times.
/// </summary>
public interface IJobMasterAssemblyDiscoveryBuilder
{
    /// <summary>Adds the assembly that contains <typeparamref name="T"/>.</summary>
    IJobMasterAssemblyDiscoveryBuilder AddAssemblyContaining<T>();

    /// <summary>Adds <paramref name="assembly"/>.</summary>
    IJobMasterAssemblyDiscoveryBuilder AddAssembly(Assembly assembly);

    /// <summary>Adds each of <paramref name="assemblies"/>.</summary>
    IJobMasterAssemblyDiscoveryBuilder AddAssemblies(params Assembly[] assemblies);
}
