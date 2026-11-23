using System.Reflection;
using JobMaster.Sdk.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Ioc.Extensions;

/// <summary>Extension methods for starting the JobMaster runtime from an <see cref="IServiceProvider"/>.</summary>
public static class JobMasterServiceProviderExtensions
{
    /// <summary>Starts the JobMaster cluster runtime. Call this from your application host startup after the DI container is built.</summary>
    public static async Task StartJobMasterRuntimeAsync(this IServiceProvider serviceProvider)
    {
        var runtime = serviceProvider.GetService<IJobMasterRuntime>();
        if (runtime is null)
        {
            // Only AddJobMasterCluster registers the runtime.
            throw new InvalidOperationException("No cluster configured. Add one with AddJobMasterCluster(...).");
        }

        await runtime.StartAsync(serviceProvider);
    }
}

