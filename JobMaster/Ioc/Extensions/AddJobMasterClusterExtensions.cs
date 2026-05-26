using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using JobMaster.Sdk.Utils;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Ioc.Extensions;

/// <summary>
/// Extension methods for registering JobMaster with the .NET dependency injection container.
/// </summary>
public static class AddJobMasterClusterExtensions
{
    /// <summary>
    /// Registers a custom JSON serializer used for job message payloads.
    /// Call this before <see cref="AddJobMasterCluster(IServiceCollection, Action{IClusterConfigSelector})"/>
    /// if you need to override the default serializer.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="serializer">The custom serializer to register.</param>
    public static void DefineJobMasterMessageJsonSerializer(this IServiceCollection services, IJobMasterMessageJsonSerializer serializer)
    {
        BootstrapBlueprintDefinitions.JobMasterJsonSerializer = serializer;
        JobMasterMessageJson.SetMessageSerializer(serializer);
    }

    /// <summary>
    /// Registers a JobMaster cluster with the specified cluster ID and configuration.
    /// Call this once per cluster during application startup.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="clusterId">Unique identifier for this cluster (letters, digits, hyphens, underscores; max 25 chars).</param>
    /// <param name="configure">Fluent configuration delegate for the cluster.</param>
    public static IServiceCollection AddJobMasterCluster(this IServiceCollection services, string clusterId, Action<IClusterConfigSelector> configure)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (configure == null) throw new ArgumentNullException(nameof(configure));

        if (!JobMasterStringUtils.IsValidForSegment(clusterId, 25))
        {
            throw new ArgumentException($"ClusterId {clusterId} is not valid for segment. It must be between 1 and 25 characters long and contain only letters, numbers, hyphens, and underscores.");
        }

        var builder = ClusterConfigSelectorAdvancedFactory.Create(clusterId, services);
        configure(builder);

        if (BootstrapBlueprintDefinitions.JobMasterJsonSerializer == null)
        {
            BootstrapBlueprintDefinitions.JobMasterJsonSerializer = new DefaultJobMasterMessageJsonSerializer();
        }
        JobMasterMessageJson.SetMessageSerializerIfNotSet(BootstrapBlueprintDefinitions.JobMasterJsonSerializer);

        if (BootstrapBlueprintDefinitions.JobMasterScheduler == null)
        {
            BootstrapBlueprintDefinitions.JobMasterScheduler = JobMasterScheduler.Instance;
        }

        if (BootstrapBlueprintDefinitions.JobMasterRuntime == null)
        {
            BootstrapBlueprintDefinitions.JobMasterRuntime = JobMasterRuntimeSingleton.Instance;
        }

        builder.Finish();

        return services;
    }

    /// <summary>
    /// Registers a JobMaster cluster using the cluster ID from configuration.
    /// Use this overload when the cluster ID is defined inside the <paramref name="configure"/> delegate
    /// via <c>ClusterId(...)</c>, or when running a single unnamed cluster.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="configure">Fluent configuration delegate for the cluster.</param>
    public static IServiceCollection AddJobMasterCluster(this IServiceCollection services, Action<IClusterConfigSelector> configure)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (configure == null) throw new ArgumentNullException(nameof(configure));

        var builder = ClusterConfigSelectorAdvancedFactory.Create(null, services);
         configure(builder);

        if (BootstrapBlueprintDefinitions.JobMasterJsonSerializer == null)
        {
            BootstrapBlueprintDefinitions.JobMasterJsonSerializer = new DefaultJobMasterMessageJsonSerializer();
        }
        JobMasterMessageJson.SetMessageSerializerIfNotSet(BootstrapBlueprintDefinitions.JobMasterJsonSerializer);

        if (BootstrapBlueprintDefinitions.JobMasterRuntime == null)
        {
            BootstrapBlueprintDefinitions.JobMasterRuntime = JobMasterRuntimeSingleton.Instance;
        }

        if (BootstrapBlueprintDefinitions.JobMasterScheduler == null)
        {
            BootstrapBlueprintDefinitions.JobMasterScheduler = JobMasterScheduler.Instance;
        }

        builder.Finish();

        return services;
    }
}
