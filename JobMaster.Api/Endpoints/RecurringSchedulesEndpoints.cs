using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JobMaster.Api.ApiModels;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Api.Endpoints;

internal static class RecurringSchedulesEndpoints
{
    internal static RouteGroupBuilder MapRecurringSchedulesEndpoints(this RouteGroupBuilder group)
    {
        var schedules = group.GetClusterEntityGroup("recurring-schedules");

        schedules.MapGet("/", QueryRecurringSchedulesAsync);
        schedules.MapGet("/count", CountRecurringSchedulesAsync);
        schedules.MapGet("/{id}", GetRecurringScheduleAsync);

        return group;
    }

    private static async Task<IResult> QueryRecurringSchedulesAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiRecurringScheduleQueryCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterRecurringSchedulesService>(clusterId);
        if (service == null)
        {
            return Results.NotFound();
        }

        RecurringScheduleQueryCriteria domainCriteria;
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
            .Select(RecurringScheduleConvertUtil.ToRecurringSchedule)
            .Select(ApiRecurringScheduleModel.FromDomain)
            .ToList();

        return Results.Ok(api);
    }

    private static Task<IResult> CountRecurringSchedulesAsync(
        [FromRoute] string clusterId,
        [AsParameters] ApiRecurringScheduleQueryCriteria criteria,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterRecurringSchedulesService>(clusterId);
        if (service == null)
        {
            return Task.FromResult(Results.NotFound() as IResult);
        }

        RecurringScheduleQueryCriteria domainCriteria;
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

    private static async Task<IResult> GetRecurringScheduleAsync(
        [FromRoute] string clusterId,
        [FromRoute] string id,
        CancellationToken ct)
    {
        var service = EndpointUtils.GetClusterAwareComponent<IMasterRecurringSchedulesService>(clusterId);
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
            return Results.BadRequest($"Invalid recurring schedule id '{id}'.");
        }

        var result = await service.GetAsync(guid);
        if (result == null)
        {
            return Results.NotFound();
        }

        var schedule = RecurringScheduleConvertUtil.ToRecurringSchedule(result);
        return Results.Ok(ApiRecurringScheduleModel.FromDomain(schedule));
    }
}
