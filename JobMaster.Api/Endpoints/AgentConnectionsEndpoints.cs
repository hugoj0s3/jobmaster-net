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
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Api.Endpoints;

internal static class AgentConnectionsEndpoints
{
    internal static RouteGroupBuilder MapAgentConnectionsEndpoints(this RouteGroupBuilder group)
    {
        var agentConnections = group.GetClusterEntityGroup("agent-connections");

        agentConnections.MapGet("/", ListAgentConnectionsAsync).Produces<List<ApiAgentConnection>>();
        agentConnections.MapGet("/count", CountAgentConnectionsAsync).Produces<int>();
        agentConnections.MapGet("/{agentConnectionId}", GetAgentConnectionAsync).Produces<ApiAgentConnection>();

        return group;
    }

    private static Task<IResult> ListAgentConnectionsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiAgentConnectionCriteria criteria,
        CancellationToken ct)
    {
        return ListAgentConnectionsCoreAsync(clusterId, criteria, ct);
    }

    private static async Task<IResult> CountAgentConnectionsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiAgentConnectionCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterAgentConnectionService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var agentConnections = await service.QueryAllAsync(useCache: true);
        return Results.Ok(agentConnections.Count);
    }

    private static async Task<IResult> ListAgentConnectionsCoreAsync(string clusterId, ApiAgentConnectionCriteria criteria, CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterAgentConnectionService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var agentConnections = await service.QueryAllAsync(useCache: true);
        var apiConnections = agentConnections
            .Select(ApiAgentConnection.FromDomain)
            .ToList();
        
        // Apply in-memory sorting
        if (criteria.SortBy != null && !string.IsNullOrWhiteSpace(criteria.SortBy.Property))
        {
            apiConnections = EndpointSortingUtil.ApplySorting(apiConnections, criteria.SortBy.Property, criteria.SortBy.Ascending);
        }
        else
        {
            // Default sorting by Name
            apiConnections = apiConnections.OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase).ToList();
        }
        
        // Apply in-memory paging
        var offset = criteria.Offset ?? 0;
        var limit = criteria.CountLimit ?? 25;
        var result = apiConnections.Skip(offset).Take(limit).ToList();

        return Results.Ok(result);
    }

    private static async Task<IResult> GetAgentConnectionAsync(
        [FromRoute] string clusterId,
        [FromRoute] string agentConnectionId,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterAgentConnectionService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }
        
        var connectionId = new AgentConnectionId(agentConnectionId);
        var agentConnection = await service.GetConnectionAsync(connectionId);
        
        if (agentConnection == null)
        {
            return Results.NotFound();
        }

        return Results.Ok(ApiAgentConnection.FromDomain(agentConnection));
    }
}
