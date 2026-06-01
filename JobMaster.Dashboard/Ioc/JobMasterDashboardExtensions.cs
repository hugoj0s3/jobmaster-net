using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.AuthRetention;
using JobMaster.Dashboard.Endpoints;
using JobMaster.Dashboard.Ioc.Selectors;
using JobMaster.Dashboard.OpenApi;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;

namespace JobMaster.Dashboard.Ioc;

public static class JobMasterDashboardExtensions
{
    /// <summary>
    /// Adds JobMaster Dashboard services to the service collection.
    /// </summary>
    public static IServiceCollection AddJobMasterDashboard(this IServiceCollection services, Action<IJobMasterDashboardBuilder> configure)
    {
        var options = new DashboardOptions();
        var builder = new JobMasterDashboardBuilder(options);
        configure(builder);

        services.AddSingleton(options);
        services.AddSingleton<OpenApiJsonConfigSeeder>();
        services.AddAuthRetention(options);

        if (options.OpenApiUrl is not null)
        {
            services.AddHttpClient();
        }

        return services;
    }

    /// <summary>
    /// Maps JobMaster Dashboard endpoints.
    /// </summary>
    public static IEndpointRouteBuilder MapJobMasterDashboard(this IEndpointRouteBuilder endpoints)
    {
        var options = endpoints.ServiceProvider.GetRequiredService<DashboardOptions>();

        var basePath = options.BasePath?.TrimEnd('/') ?? string.Empty;
        if (!string.IsNullOrEmpty(basePath) && !basePath.StartsWith("/"))
        {
            basePath = "/" + basePath;
        }

        endpoints
            .MapDashboardConfigEndpoints(basePath)
            .MapDashboardAuthRetentionEndpoints(basePath);

        endpoints.MapGet($"{basePath}/debug-resources", () =>
            typeof(JobMasterDashboardExtensions).Assembly.GetManifestResourceNames());

        var assembly = typeof(JobMasterDashboardExtensions).Assembly;
        var provider = new ManifestEmbeddedFileProvider(assembly, "Embedded");

        // StaticFileOptions.RequestPath strictly requires a leading slash.
        endpoints.MapFallbackToFile(
            $"{basePath}/{{**slug}}",
            "index.html",
            new StaticFileOptions { RequestPath = basePath, FileProvider = provider });

        return endpoints;
    }

    private static IServiceCollection AddAuthRetention(this IServiceCollection services, DashboardOptions options)
    {
        switch (options.AuthRetention.AuthRetentionType)
        {
            case DashboardAuthRetentionType.ServerSideInMemory:
                services.AddMemoryCache();
                services.AddSingleton<IAuthRetentionService, InMemoryAuthRetentionService>();
                break;
            case DashboardAuthRetentionType.ServerSideDistributed:
                services.AddSingleton<IAuthRetentionService, DistributedAuthRetentionService>();
                break;
            default:
                services.AddSingleton<IAuthRetentionService, NullAuthRetentionService>();
                break;
        }

        return services;
    }

}
