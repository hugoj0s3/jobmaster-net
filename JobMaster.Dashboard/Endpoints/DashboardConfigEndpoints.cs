using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Public;
using JobMaster.Dashboard.OpenApi;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Dashboard.Endpoints;

internal static class DashboardConfigEndpoints
{
    internal static IEndpointRouteBuilder MapDashboardConfigEndpoints(this IEndpointRouteBuilder endpoints, string basePath)
    {
        endpoints.MapGet($"{basePath}/jobmaster-config.json", async (
            HttpContext ctx,
            DashboardOptions options,
            OpenApiJsonConfigSeeder seeder) =>
        {
            await seeder.EnsureSeededAsync(options, ctx);

            var config = DashboardPublicConfigConvertUtil.ToPublicConfig(options);
            return Results.Ok(config);
        });

        return endpoints;
    }
}
