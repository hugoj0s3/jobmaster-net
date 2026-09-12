using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Ioc.Extensions;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Ioc.Selectors;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Api.AspNetCore;

/// <summary>
/// Extension methods for registering JobMaster clusters in API-only mode.
/// </summary>
public static class JobMasterClusterApiExtensions
{
    /// <summary>
    /// Registers a JobMaster cluster for API-only access (monitoring and operations without job processing).
    /// Only the cluster identity and master database connection are required — no workers or agent connections needed.
    /// Use this when deploying the <c>JobMaster.Api</c> server as a standalone process that connects
    /// directly to the master database without running any jobs.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="clusterId">Unique identifier for this cluster (letters, digits, hyphens, underscores; max 25 chars).</param>
    /// <param name="configure">Fluent configuration delegate. Use a provider extension (e.g. <c>UsePostgresForMaster</c>) to supply the master database connection.</param>
    public static IServiceCollection AddJobMasterClusterForApi(this IServiceCollection services, string clusterId, Action<IBaseClusterConfigSelector<IClusterConfigSelector>> configure)
    {
        return AddJobMasterClusterExtensions.AddJobMasterCluster(services, clusterId, selector =>
        {
            configure(selector);
            EnsureApiDefaultSet(selector);
        });
    }

    /// <summary>
    /// Registers a JobMaster cluster for API-only access (monitoring and operations without job processing).
    /// Only the cluster identity and master database connection are required — no workers or agent connections needed.
    /// Use this overload when the cluster ID is set inside the <paramref name="configure"/> delegate via <c>ClusterId(...)</c>.
    /// </summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="configure">Fluent configuration delegate. Call <c>ClusterId(...)</c> and a provider extension (e.g. <c>UsePostgresForMaster</c>) to configure the connection.</param>
    public static IServiceCollection AddJobMasterClusterForApi(this IServiceCollection services, Action<IBaseClusterConfigSelector<IClusterConfigSelector>> configure)
    {
        return AddJobMasterClusterExtensions.AddJobMasterCluster(services, selector =>
        {
            configure(selector);
            EnsureApiDefaultSet(selector);
        });
    }

    // When all clusters are API-only, no cluster will be marked as default and startup throws.
    // The default has no meaning for API routing (routes are always cluster-ID-aware), so we silently
    // promote the first API cluster to default to satisfy the runtime invariant.
    private static void EnsureApiDefaultSet(IClusterConfigSelector selector)
    {
        if (!BootstrapBlueprintDefinitions.Clusters.Any(c => c.IsDefault))
            selector.SetAsDefault();
    }
}
