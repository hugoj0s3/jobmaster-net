using JobMaster.Abstractions.Ioc.Selectors;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Abstractions.Ioc.Selectors;

internal static class ClusterConfigSelectorAdvancedFactory
{
    public static IClusterConfigSelector Create(string? clusterId, IServiceCollection serviceCollection)
    {
        // Try to resolve the concrete builder type across assemblies
        var typeName = "JobMaster.Sdk.Ioc.Setup.ClusterConfigBuilder, JobMaster.Sdk";
        var builderType = Type.GetType(typeName);

        if (builderType == null)
        {
            // Fallback: scan loaded assemblies for the type
            builderType = AppDomain.CurrentDomain
                .GetAssemblies()
                .Select(a => a.GetType("JobMaster.Sdk.Ioc.Setup.ClusterConfigBuilder", throwOnError: false, ignoreCase: false))
                .FirstOrDefault(t => t != null);
        }

        var constructor = builderType
            !
            .GetConstructor(new[] { typeof(string), typeof(IServiceCollection) })
            !;

        return (IClusterConfigSelector)constructor.Invoke([clusterId, serviceCollection]);
    }
}
