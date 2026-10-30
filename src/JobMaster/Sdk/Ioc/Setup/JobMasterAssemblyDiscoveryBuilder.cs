using System.Reflection;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Ioc;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace JobMaster.Sdk.Ioc.Setup;

internal sealed class JobMasterAssemblyDiscoveryBuilder : IJobMasterAssemblyDiscoveryBuilder
{
    private readonly IServiceCollection services;

    public JobMasterAssemblyDiscoveryBuilder(IServiceCollection services)
    {
        this.services = services;
    }

    public IJobMasterAssemblyDiscoveryBuilder AddAssemblyContaining<T>() => AddAssembly(typeof(T).Assembly);

    public IJobMasterAssemblyDiscoveryBuilder AddAssembly(Assembly assembly)
    {
        if (assembly == null) throw new ArgumentNullException(nameof(assembly));
        if (assembly.IsDynamic)
        {
            throw new ArgumentException($"Assembly {assembly.FullName} is dynamic; JobMaster can't discover types in it.", nameof(assembly));
        }

        TypeDiscovery.AddAssembly(assembly);

        // Registered here too, so the handlers resolve even when this runs after AddJobMasterCluster's own scan.
        foreach (var handlerType in typeof(IJobMasterHandler).GetCachedConcreteTypesOf(new[] { assembly }))
        {
            services.TryAddScoped(handlerType);
        }

        return this;
    }

    public IJobMasterAssemblyDiscoveryBuilder AddAssemblies(params Assembly[] assemblies)
    {
        if (assemblies == null) throw new ArgumentNullException(nameof(assemblies));

        foreach (var assembly in assemblies)
        {
            AddAssembly(assembly);
        }

        return this;
    }
}
