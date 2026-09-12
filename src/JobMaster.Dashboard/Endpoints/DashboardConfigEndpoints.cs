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
            try
            {
                await seeder.EnsureSeededAsync(options, ctx);
            }
            catch (Exception ex)
            {
                var httpEx = (ex as HttpRequestException) ?? (ex.InnerException as HttpRequestException);
                if (httpEx?.StatusCode == System.Net.HttpStatusCode.NotFound)
                    return Results.NotFound();
                throw;
            }

            var config = DashboardPublicConfigConvertUtil.ToPublicConfig(options);
            return Results.Ok(config);
        }).ExcludeFromDescription();

        return endpoints;
    }
}
