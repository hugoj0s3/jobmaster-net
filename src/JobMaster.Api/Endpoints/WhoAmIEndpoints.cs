using JobMaster.Api.ApiModels;
using JobMaster.Api.AspNetCore;
using JobMaster.Api.AspNetCore.Auth;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Api.Endpoints;

internal static class WhoAmIEndpoints
{
    internal static RouteGroupBuilder MapWhoAmIEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/whoami", GetWhoAmI).Produces<ApiWhoAmIModel>();

        return group;
    }

    private static IResult GetWhoAmI(HttpContext httpContext)
    {
        var identity = JobMasterApiEndpointRouteBuilderExtensions.GetResolvedIdentity(httpContext);

        return Results.Ok(new ApiWhoAmIModel
        {
            Subject = identity?.Subject,
            AuthenticationType = identity?.AuthenticationType
        });
    }
}
