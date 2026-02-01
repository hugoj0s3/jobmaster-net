using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Api.Endpoints;

internal static class WorkersEndpoints
{
    internal static RouteGroupBuilder MapWorkersEndpoints(this RouteGroupBuilder group)
    {
        var workers = group.GetClusterEntityGroup("workers");

        workers.MapGet("/", QueryWorkersAsync);
        workers.MapGet("/count", CountWorkersAsync);
        workers.MapGet("/{workerId}", GetWorkerAsync);

        return group;
    }

    private static async Task<IResult> QueryWorkersAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiAgentWorkerCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterAgentWorkersService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var workers = await service.QueryWorkersAsync(useCache: false);

        if (!string.IsNullOrEmpty(criteria.WorkerLane))
            workers = workers.Where(w => string.Equals(w.WorkerLane, criteria.WorkerLane, StringComparison.Ordinal)).ToList();
        if (criteria.Mode.HasValue)
            workers = workers.Where(w => w.Mode == criteria.Mode.Value).ToList();
        if (criteria.IsAlive.HasValue)
            workers = workers.Where(w => w.IsAlive == criteria.IsAlive.Value).ToList();
        if (criteria.Status.HasValue)
            workers = workers.Where(w => w.Status() == criteria.Status.Value).ToList();
        if (!string.IsNullOrEmpty(criteria.AgentConnectionId))
            workers = workers.Where(w => string.Equals(w.AgentConnectionId.IdValue, criteria.AgentConnectionId, StringComparison.OrdinalIgnoreCase)).ToList();

        var result = workers.Select(ApiAgentWorker.FromDomain).ToList();

        return Results.Ok(result);
    }

    private static async Task<IResult> CountWorkersAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiAgentWorkerCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterAgentWorkersService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var workers = await service.QueryWorkersAsync(useCache: true);

        if (!string.IsNullOrEmpty(criteria.WorkerLane))
            workers = workers.Where(w => string.Equals(w.WorkerLane, criteria.WorkerLane, StringComparison.OrdinalIgnoreCase)).ToList();
        if (criteria.Mode.HasValue)
            workers = workers.Where(w => w.Mode == criteria.Mode.Value).ToList();
        if (criteria.IsAlive.HasValue)
            workers = workers.Where(w => w.IsAlive == criteria.IsAlive.Value).ToList();
        if (criteria.Status.HasValue)
            workers = workers.Where(w => w.Status() == criteria.Status.Value).ToList();
        if (!string.IsNullOrEmpty(criteria.AgentConnectionId))
            workers = workers.Where(w => string.Equals(w.AgentConnectionId.IdValue, criteria.AgentConnectionId, StringComparison.OrdinalIgnoreCase)).ToList();

        return Results.Ok(workers.Count);
    }

    private static async Task<IResult> GetWorkerAsync(
        [FromRoute] string clusterId,
        string workerId,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterAgentWorkersService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        var worker = await service.GetWorkerAsync(workerId);
        if (worker == null)
        {
            return Results.NotFound();
        }

        return Results.Ok(ApiAgentWorker.FromDomain(worker));
    }
}
