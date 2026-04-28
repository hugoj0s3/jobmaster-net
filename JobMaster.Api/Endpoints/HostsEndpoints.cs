using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Api.Endpoints;

internal static class HostsEndpoints
{
    internal static RouteGroupBuilder MapHostsEndpoints(this RouteGroupBuilder group)
    {
        var hosts = group.GetClusterEntityGroup("hosts");

        hosts.MapGet("/", ListHostsAsync);
        hosts.MapGet("/count", CountHostsAsync);
        hosts.MapGet("/{hostId}", GetHostAsync);

        return group;
    }

    private static async Task<IResult> CountHostsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiHostCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterHostService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var hosts = await service.QueryAllAsync();
        return Results.Ok(hosts.Count);
    }

    private static async Task<IResult> ListHostsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiHostCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterHostService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var hosts = await service.QueryAllAsync();
        var apiHosts = hosts.Select(ApiHostModel.FromDomain).ToList();
        
        // Apply in-memory sorting
        if (criteria.SortBy != null && !string.IsNullOrWhiteSpace(criteria.SortBy.Property))
        {
            apiHosts = EndpointSortingUtil.ApplySorting(apiHosts, criteria.SortBy.Property, criteria.SortBy.Ascending);
        }
        else
        {
            // Default sorting by HostDisplayName
            apiHosts = apiHosts.OrderBy(x => x.HostDisplayName, StringComparer.OrdinalIgnoreCase).ToList();
        }
        
        // Apply in-memory paging
        var offset = criteria.Offset ?? 0;
        var limit = criteria.CountLimit ?? 25;
        var result = apiHosts.Skip(offset).Take(limit).ToList();
        
        return Results.Ok(result);
    }

    private static async Task<IResult> GetHostAsync(
        [FromRoute] string clusterId,
        [FromRoute] string hostId,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterHostService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var hosts = await service.QueryAllAsync();
        var host = hosts.FirstOrDefault(h => string.Equals(h.Id.IdValue, hostId, StringComparison.OrdinalIgnoreCase));
        if (host == null)
        {
            return Results.NotFound();
        }

        return Results.Ok(ApiHostModel.FromDomain(host));
    }

}
