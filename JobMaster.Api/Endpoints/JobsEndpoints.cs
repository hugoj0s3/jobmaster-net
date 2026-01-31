using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Api.Endpoints;

internal static class JobsEndpoints
{
    internal static RouteGroupBuilder MapJobsEndpoints(this RouteGroupBuilder group)
    {
        var jobs = group.GetClusterEntityGroup("jobs");

        jobs.MapGet("/", QueryJobsAsync);
        jobs.MapGet("/count", CountJobsAsync);
        jobs.MapGet("/{id}", GetJobAsync);

        return group;
    }

    private static async Task<IResult> QueryJobsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiJobQueryCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterJobsService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        JobQueryCriteria domainCriteria;
        try
        {
            domainCriteria = criteria.ToDomainCriteria();
        }
        catch (Exception)
        {
            return Results.BadRequest("Invalid query criteria.");
        }

        var result = await service.QueryAsync(domainCriteria);
        var api = result
            .Select(x => Job.FromModel(x))
            .Select(ApiJobModel.FromDomain)
            .ToList();

        return Results.Ok(api);
    }

    private static Task<IResult> CountJobsAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiJobQueryCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterJobsService>(clusterId);
        if (service == null)
        {
            return Task.FromResult(Results.NotFound() as IResult);
        }

        JobQueryCriteria domainCriteria;
        try
        {
            domainCriteria = criteria.ToDomainCriteria();
        }
        catch (Exception)
        {
            return Task.FromResult(Results.BadRequest("Invalid query criteria.") as IResult);
        }

        var result = service.Count(domainCriteria);
        return Task.FromResult(Results.Ok(result) as IResult);
    }

    private static async Task<IResult> GetJobAsync(
        [FromRoute] string clusterId,
        [FromRoute] string id,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterJobsService>(clusterId);
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
            return Results.BadRequest($"Invalid job id '{id}'.");
        }

        var result = await service.GetAsync(guid);
        if (result == null)
        {
            return Results.NotFound();
        }

        var job = Job.FromModel(result);
        return Results.Ok(ApiJobModel.FromDomain(job));
    }
}
