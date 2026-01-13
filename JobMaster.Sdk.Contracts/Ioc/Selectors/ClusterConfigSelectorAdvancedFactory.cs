using JobMaster.Contracts.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Contracts.Ioc.Selectors;

public static class ClusterConfigSelectorAdvancedFactory
{
    public static IClusterConfigSelectorAdvanced Create(string? clusterId, IServiceCollection serviceCollection)
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
            .NotNull()
            .GetConstructor(new[] { typeof(string), typeof(IServiceCollection) })
            .NotNull();

        return (IClusterConfigSelectorAdvanced)constructor.Invoke([clusterId, serviceCollection]);
    }
}
