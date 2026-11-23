using JobMaster.Abstractions.Ioc;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Ioc.Extensions;

/// <summary>Extension methods for adding assemblies to JobMaster's type discovery.</summary>
public static class JobMasterAssemblyDiscoveryExtensions
{
    /// <summary>
    /// Adds assemblies for JobMaster to discover handlers, static recurring schedule profiles and job definitions in.
    /// Use it for a library your code never references directly, for example:
    /// <code>services.JobMasterAssemblyDiscovery().AddAssemblyContaining&lt;BillingHandler&gt;();</code>
    /// </summary>
    /// <param name="services">The application service collection.</param>
    public static IJobMasterAssemblyDiscoveryBuilder JobMasterAssemblyDiscovery(this IServiceCollection services)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));

        return new JobMasterAssemblyDiscoveryBuilder(services);
    }
}
