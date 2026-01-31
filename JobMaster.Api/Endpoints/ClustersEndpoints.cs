using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Api.Endpoints;

internal static class ClustersEndpoints
{
    internal static RouteGroupBuilder MapClustersEndpoints(this RouteGroupBuilder group)
    {
        var clusters = group.MapGroup("/clusters").WithTags("Clusters");

        clusters.MapGet("/count", GetClustersCount);
        clusters.MapGet("/ids", GetClusterIds);
        clusters.MapGet("/{clusterId}", GetClusterDetailAsync);

        return group;
    }

    private static IResult GetClustersCount(CancellationToken ct)
    {
        return Results.Ok(JobMasterClusterConnectionConfig.ClusterCount);
    }

    private static IResult GetClusterIds(CancellationToken ct)
    {
        var result = JobMasterClusterConnectionConfig.GetActiveConfigs().Select(x => x.ClusterId).ToList();
        return Results.Ok(result);
    }

    private static async Task<IResult> GetClusterDetailAsync(
        [FromRoute] string clusterId,
        CancellationToken ct)
    {
        var clusterConnConfig = JobMasterClusterConnectionConfig.TryGet(clusterId, includeInactive: true);
        if (clusterConnConfig == null)
        {
            return Results.NotFound();
        }

        var service = EndpointUtils.GetClusterAwareComponent<IMasterClusterConfigurationService>(clusterConnConfig.ClusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var model = service.Get() ?? new ClusterConfigurationModel(clusterConnConfig.ClusterId);
        var api = new ApiClusterModel
        {
            ClusterId = clusterConnConfig.ClusterId,
            RepositoryTypeId = clusterConnConfig.RepositoryTypeId,
            DefaultJobTimeout = model.DefaultJobTimeout,
            TransientThreshold = model.TransientThreshold,
            DefaultMaxOfRetryCount = model.DefaultMaxOfRetryCount,
            ClusterMode = model.ClusterMode,
            MaxMessageByteSize = model.MaxMessageByteSize,
            IanaTimeZoneId = model.IanaTimeZoneId,
            DataRetentionTtl = model.DataRetentionTtl,
            AdditionalConfig = model.AdditionalConfig.GetFullDictionary(),
        };

        return Results.Ok(api);
    }
}