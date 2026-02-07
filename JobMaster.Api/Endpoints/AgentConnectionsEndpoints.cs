using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using System;
using System.Collections.Generic;
using System.Linq;
using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Api.Endpoints;

internal static class AgentConnectionsEndpoints
{
    internal static RouteGroupBuilder MapAgentConnectionsEndpoints(this RouteGroupBuilder group)
    {
        var agentConnections = group.GetClusterEntityGroup("agent-connections");

        // TODO: This is a derived/mock endpoint (derived from buckets; footprint is mocked) until agent-connection entity is implemented.

        agentConnections.MapGet("/", ListAgentConnectionsAsync);
        agentConnections.MapGet("/count", CountAgentConnectionsAsync);
        agentConnections.MapGet("/{agentConnectionId}", GetAgentConnectionAsync);

        return group;
    }

    private static async Task<List<ApiAgentConnection>> QueryAgentConnectionsFromBucketsAsync(string clusterId)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterBucketsService>(clusterId);
        if (service == null)
        {
            return [];
        }

        var criteria = new MasterBucketQueryCriteria
        {
            ReadIsolationLevel = ReadIsolationLevel.FastSync,
        };

        var buckets = await service.QueryAsync(criteria);

        var distinct = buckets
            .Where(b => b.AgentConnectionId != null && !string.IsNullOrWhiteSpace(b.AgentConnectionId.IdValue))
            .Select(b => new { b.AgentConnectionId!.IdValue, b.AgentConnectionId.Name, b.RepositoryTypeId })
            .DistinctBy(x => x.IdValue)
            .OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var seed = clusterId?.GetHashCode(StringComparison.Ordinal) ?? 0;
        var rng = new Random(seed ^ 0x51A7C0DE);

        var result = new List<ApiAgentConnection>(distinct.Count);
        foreach (var item in distinct)
        {
            var footprintSuffix = rng.Next(1000, 9999);
            result.Add(new ApiAgentConnection
            {
                Id = item.IdValue,
                Name = item.Name,
                RepositoryTypeId = item.RepositoryTypeId,
                FootPrint = $"mock-{item.RepositoryTypeId}-{footprintSuffix}"
            });
        }

        return result;
    }

    private static Task<IResult> ListAgentConnectionsAsync(
        [FromRoute] string clusterId,
        CancellationToken ct)
    {
        return ListAgentConnectionsCoreAsync(clusterId);
    }

    private static async Task<IResult> CountAgentConnectionsAsync(
        [FromRoute] string clusterId,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterBucketsService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var result = await QueryAgentConnectionsFromBucketsAsync(clusterId);
        return Results.Ok(result.Count);
    }

    private static async Task<IResult> ListAgentConnectionsCoreAsync(string clusterId)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterBucketsService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var result = await QueryAgentConnectionsFromBucketsAsync(clusterId);
        return Results.Ok(result);
    }

    private static async Task<IResult> GetAgentConnectionAsync(
        [FromRoute] string clusterId,
        [FromRoute] string agentConnectionId,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterBucketsService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var list = await QueryAgentConnectionsFromBucketsAsync(clusterId);
        var item = list.FirstOrDefault(x => string.Equals(x.Id, agentConnectionId, StringComparison.OrdinalIgnoreCase));
        return item == null ? Results.NotFound() : Results.Ok(item);
    }
}
