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
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterHostService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var hosts = await service.QueryAllAsync();
        var result = hosts.Select(ApiHostModel.FromDomain).ToList();
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
