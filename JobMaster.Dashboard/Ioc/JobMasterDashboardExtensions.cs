using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Credentials;
using JobMaster.Dashboard.Endpoints;
using JobMaster.Dashboard.Ioc.Selectors;
using JobMaster.Dashboard.OpenApi;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Caching.Distributed;
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
        services.AddCredentialsPersistence(options);
        services.AddRateSessionLimiter(options);

        if (options.OpenApiUrl is not null)
        {
            services.AddHttpClient();
            services.AddHostedService<OpenApiConfigSeedService>();
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
            .MapDashboardCredentialsEndpoints(basePath);

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

    private static IServiceCollection AddCredentialsPersistence(this IServiceCollection services, DashboardOptions options)
    {
        switch (options.CredentialsPersistence.PersistenceType)
        {
            case DashboardCredentialsPersistenceType.ServerSideInMemory:
                services.AddMemoryCache();
                services.AddSingleton<ICredentialsPersistenceService, InMemoryCredentialsPersistenceService>();
                break;
            case DashboardCredentialsPersistenceType.ServerSideDistributed:
                services.AddSingleton<ICredentialsPersistenceService, DistributedCredentialsPersistenceService>();
                break;
            default:
                services.AddSingleton<ICredentialsPersistenceService, NullCredentialsPersistenceService>();
                break;
        }

        return services;
    }

    private static IServiceCollection AddRateSessionLimiter(this IServiceCollection services, DashboardOptions options)
    {
        var config = options.CredentialsPersistence;

        if (config.PersistenceType == DashboardCredentialsPersistenceType.ServerSideDistributed)
        {
            services.AddSingleton<IRateSessionLimiter>(sp =>
                new DistributedRateSessionLimiter(
                    sp.GetRequiredService<IDistributedCache>(),
                    max: DashboardCredentialsPersistenceConfig.OpenSessionRateLimit,
                    window: DashboardCredentialsPersistenceConfig.OpenSessionRateWindow));
        }
        else
        {
            services.AddSingleton<IRateSessionLimiter>(
                new LocalRateSessionLimiter(
                    max: DashboardCredentialsPersistenceConfig.OpenSessionRateLimit,
                    window: DashboardCredentialsPersistenceConfig.OpenSessionRateWindow));
        }

        return services;
    }
}
