using System.ComponentModel;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sdk.Abstractions.Ioc.Selectors;

[EditorBrowsable(EditorBrowsableState.Never)]
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
            !
            .GetConstructor(new[] { typeof(string), typeof(IServiceCollection) })
            !;

        return (IClusterConfigSelectorAdvanced)constructor.Invoke([clusterId, serviceCollection]);
    }
}
