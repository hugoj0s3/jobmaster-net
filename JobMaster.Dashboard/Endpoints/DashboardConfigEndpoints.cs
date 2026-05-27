using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Public;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Dashboard.Endpoints;

internal static class DashboardConfigEndpoints
{
    internal static IEndpointRouteBuilder MapDashboardConfigEndpoints(this IEndpointRouteBuilder endpoints, string basePath)
    {
        endpoints.MapGet($"{basePath}/jobmaster-config.json", (DashboardOptions options) =>
        {
            var config = DashboardPublicConfigConvertUtil.ToPublicConfig(options);
            return Results.Ok(config);
        });

        return endpoints;
    }
}
