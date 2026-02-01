using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Api.Endpoints;

internal static class BucketsEndpoints
{
    internal static RouteGroupBuilder MapBucketsEndpoints(this RouteGroupBuilder group)
    {
        var buckets = group.GetClusterEntityGroup("buckets");

        buckets.MapGet("/", QueryBucketsAsync);
        buckets.MapGet("/count", CountBucketsAsync);
        buckets.MapGet("/{bucketId}", GetBucketAsync);

        return group;
    }

    private static async Task<IResult> QueryBucketsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiMasterBucketQueryCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterBucketsService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }
        
        var result = await service.QueryAsync(criteria.ToDomainCriteria());
        return Results.Ok(result.Select(ApiBucketModel.FromDomain).ToList());
    }

    private static async Task<IResult> CountBucketsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiMasterBucketQueryCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterBucketsService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }
        
        var result = await service.CountAsync(criteria.ToDomainCriteria());
        return Results.Ok(result);
    }

    private static async Task<IResult> GetBucketAsync(
        [FromRoute] string clusterId,
        string bucketId,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterBucketsService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        } 
        
        var result = service.Get(bucketId, JobMasterConstants.BucketFastAllowDiscrepancy);
        if (result == null)
        {
            return Results.NotFound();
        }
        
        return Results.Ok(ApiBucketModel.FromDomain(result));
    }
}