using System;
using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Api.Endpoints;

internal static class LogsEndpoints
{
    internal static RouteGroupBuilder MapLogsEndpoints(this RouteGroupBuilder group)
    {
        var logs = group.GetClusterEntityGroup("logs");

        logs.MapGet("/", QueryLogsAsync);
        logs.MapGet("/{id}", GetLogAsync);
        logs.MapGet("/count", CountLogsAsync);

        return group;
    }

    private static async Task<IResult> QueryLogsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiLogItemQueryCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IJobMasterLogger>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var result = await service.QueryAsync(criteria.ToDomainCriteria());
        var apiLogItems = result.Select(ApiLogItem.FromDomain).ToList();
        foreach (var apiLogItem in apiLogItems)
        {
            apiLogItem.CutMessage();
        }
        
        return Results.Ok(apiLogItems);
    }

    private static async Task<IResult> CountLogsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiLogItemQueryCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IJobMasterLogger>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var result = await service.CountAsync(criteria.ToDomainCriteria());
        return Results.Ok(result);
    }

    private static async Task<IResult> GetLogAsync(
        [FromRoute] string clusterId,
        [FromRoute] string id,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IJobMasterLogger>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        Guid guid;
        try
        {
            guid = id.FromBase64();
        }
        catch (Exception)
        {
            return Results.BadRequest($"Invalid log id '{id}'.");
        }

        var result = await service.GetAsync(guid);
        if (result == null)
        {
            return Results.NotFound();
        }

        return Results.Ok(ApiLogItem.FromDomain(result));
    }
}
