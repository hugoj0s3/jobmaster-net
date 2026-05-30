using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using System.Collections.Generic;

namespace JobMaster.Api.Endpoints;

internal static class BucketsEndpoints
{
    internal static RouteGroupBuilder MapBucketsEndpoints(this RouteGroupBuilder group)
    {
        var buckets = group.GetClusterEntityGroup("buckets");

	buckets.MapGet("/", QueryBucketsAsync).Produces<List<ApiBucketModel>>();
        buckets.MapGet("/count", CountBucketsAsync).Produces<int>();
        buckets.MapGet("/{bucketId}", GetBucket).Produces<ApiBucketModel>();

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
        
        var buckets = await service.QueryAsync(
            criteria.ToDomainCriteria(), 
            allowedDiscrepancy: JobMasterConstants.BucketFastAllowDiscrepancy);
        
        var apiBuckets = buckets.Select(ApiBucketModel.FromDomain).ToList();
        
        // Apply in-memory sorting
        if (criteria.SortBy != null && !string.IsNullOrWhiteSpace(criteria.SortBy.Property))
        {
            apiBuckets = EndpointSortingUtil.ApplySorting(apiBuckets, criteria.SortBy.Property, criteria.SortBy.Ascending);
        }
        
        // Apply in-memory paging
        var offset = criteria.Offset ?? 0;
        var limit = criteria.CountLimit ?? 25;
        var result = apiBuckets.Skip(offset).Take(limit).ToList();
        
        return Results.Ok(result);
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
        
        var buckets = await service.QueryAsync(criteria.ToDomainCriteria());
        return Results.Ok(buckets.Count);
    }

    private static IResult GetBucket(
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
