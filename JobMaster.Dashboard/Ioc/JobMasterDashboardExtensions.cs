using System.Text.RegularExpressions;
using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.AuthRetention;
using JobMaster.Dashboard.Endpoints;
using JobMaster.Dashboard.Ioc.Selectors;
using JobMaster.Dashboard.OAuthFlow;
using JobMaster.Dashboard.OpenApi;
using JobMaster.Dashboard.Middleware;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
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

        var oauthConfig = options.Auth.Providers.OfType<OAuthAuthConfig>().FirstOrDefault();
        ValidateOAuthConfig(oauthConfig);

        services.AddSingleton(options);
        services.AddSingleton<OpenApiJsonConfigSeeder>();
        services.AddAuthRetention(options);

        var hasOAuthProviders = oauthConfig is { Disabled: false } && oauthConfig.Providers.Count > 0;

        if (options.OpenApiUrl is not null || hasOAuthProviders)
        {
            services.AddHttpClient();
        }

        if (hasOAuthProviders)
        {
            services.AddOAuthFlowStateStorage(oauthConfig!);
        }

        return services;
    }

    /// <summary>
    /// Registers the JobMaster Dashboard middleware and maps all dashboard endpoints.
    /// </summary>
    public static WebApplication StartJobMasterDashboard(this WebApplication app)
    {
        app.UseJobMasterDashboard();
        app.MapJobMasterDashboard();
        return app;
    }

    internal static IEndpointRouteBuilder MapJobMasterDashboard(this IEndpointRouteBuilder endpoints)
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

        var oauthConfig = options.Auth.Providers.OfType<OAuthAuthConfig>().FirstOrDefault();
        if (oauthConfig is { Disabled: false } && oauthConfig.Providers.Count > 0)
        {
            endpoints.MapDashboardOAuthEndpoints(basePath);
        }

        endpoints.MapGet($"{basePath}/debug-resources", () =>
            typeof(JobMasterDashboardExtensions).Assembly.GetManifestResourceNames())
            .ExcludeFromDescription();

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
                services.AddSingleton<IJobMasterAuthRetentionStorage, InMemoryAuthRetentionStorage>();
                break;
            case DashboardAuthRetentionType.ServerSideDistributed:
                services.AddSingleton<IJobMasterAuthRetentionStorage, DistributedAuthRetentionStorage>();
                break;
            case DashboardAuthRetentionType.Custom:
                if (options.AuthRetention.CustomAuthRetentionStorageType is not { } customType)
                    throw new InvalidOperationException(
                        "JobMaster Dashboard: AuthRetentionType is Custom but no type was registered. " +
                        "Call ConfigureAuthRetention().UseCustom<T>() instead of SetAuthRetentionType(Custom) directly.");
                services.AddSingleton(typeof(IJobMasterAuthRetentionStorage), customType);
                break;
            default:
                services.AddSingleton<IJobMasterAuthRetentionStorage, NullAuthRetentionStorage>();
                break;
        }

        return services;
    }

    private static IServiceCollection AddOAuthFlowStateStorage(this IServiceCollection services, OAuthAuthConfig oauthConfig)
    {
        switch (oauthConfig.FlowStateStorage)
        {
            case OAuthFlowStateStorage.InMemory:
                services.AddMemoryCache();
                services.AddSingleton<IJobMasterOAuthFlowStateStorage, InMemoryOAuthFlowStateStorage>();
                break;
            case OAuthFlowStateStorage.Distributed:
                services.AddSingleton<IJobMasterOAuthFlowStateStorage, DistributedOAuthFlowStateStorage>();
                break;
            case OAuthFlowStateStorage.Custom:
                if (oauthConfig.CustomFlowStateStorageType is not { } customType)
                    throw new InvalidOperationException(
                        "JobMaster Dashboard: OAuth flow-state storage is Custom but no type was registered. " +
                        "Call ConfigOAuth().UseCustomStorage<T>() instead of WithStorage(Custom) directly.");
                services.AddSingleton(typeof(IJobMasterOAuthFlowStateStorage), customType);
                break;
            default:
                services.AddDataProtection();
                services.AddSingleton<IJobMasterOAuthFlowStateStorage, CookieOAuthFlowStateStorage>();
                break;
        }

        return services;
    }

    private static readonly Regex OAuthKeyPattern = new("^[a-zA-Z0-9][a-zA-Z0-9-_]*$", RegexOptions.Compiled);

    private static void ValidateOAuthConfig(OAuthAuthConfig? oauthConfig)
    {
        if (oauthConfig is null || oauthConfig.Providers.Count == 0) return;

        foreach (var provider in oauthConfig.Providers)
        {
            if (string.IsNullOrEmpty(provider.Key) || !OAuthKeyPattern.IsMatch(provider.Key))
                throw new InvalidOperationException(
                    $"JobMaster Dashboard: invalid OAuth provider key '{provider.Key}'. " +
                    "Keys must be non-empty and URL-safe (letters, digits, '-', '_').");
        }

        var duplicateKeys = oauthConfig.Providers
            .GroupBy(p => p.Key, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();
        if (duplicateKeys.Count > 0)
            throw new InvalidOperationException(
                $"JobMaster Dashboard: duplicate OAuth provider key(s): {string.Join(", ", duplicateKeys)}. " +
                "Each AddOAuthProvider key must be unique.");

        if (!oauthConfig.Disabled && oauthConfig.TokenIssuer is null)
            throw new InvalidOperationException(
                "JobMaster Dashboard: an OAuth provider is configured but no token issuer was set. " +
                "Call ConfigOAuth().WithTokenIssuer(...) to mint a JobMaster-aligned JWT from the " +
                "identity an OAuth login establishes.");
    }
}
