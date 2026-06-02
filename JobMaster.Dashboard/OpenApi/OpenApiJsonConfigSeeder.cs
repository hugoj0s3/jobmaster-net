using System.Text.Json;
using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Auth;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Dashboard.OpenApi;

internal sealed class OpenApiJsonConfigSeeder
{
    private int isSeeded;
    private readonly SemaphoreSlim seederLock = new(1, 1);

    public async Task EnsureSeededAsync(DashboardOptions options, HttpContext ctx)
    {
        if (options.OpenApiUrl is null) return;
        if (isSeeded == 1) return;

        await seederLock.WaitAsync();
        try
        {
            if (isSeeded == 1) return;

            var httpClientFactory = ctx.RequestServices.GetRequiredService<IHttpClientFactory>();
            await SeedAsync(options, ctx, httpClientFactory);

            isSeeded = 1;
        }
        finally
        {
            seederLock.Release();
        }
    }

    private static async Task SeedAsync(DashboardOptions options, HttpContext ctx, IHttpClientFactory httpClientFactory)
    {
        var rawUrlOrPath = options.OpenApiUrl!;

        if (IsAbsoluteUrl(rawUrlOrPath))
        {
            var json = await FetchHttpAsync(rawUrlOrPath, httpClientFactory);
            Apply(options, json);
        }
        else if (HasJsonExtension(rawUrlOrPath))
        {
            var env = ctx.RequestServices.GetRequiredService<IWebHostEnvironment>();
            var fullPath = Path.IsPathRooted(rawUrlOrPath)
                ? rawUrlOrPath
                : Path.Combine(env.ContentRootPath, rawUrlOrPath.TrimStart('/', '\\', '.'));

            if (!File.Exists(fullPath))
                throw new InvalidOperationException($"JobMaster Dashboard: OpenAPI JSON file not found at '{fullPath}'.");

            var content = await File.ReadAllTextAsync(fullPath);
            Apply(options, content);
        }
        else
        {
            var baseApiUrl = string.IsNullOrEmpty(rawUrlOrPath) ? options.ApiUrl : rawUrlOrPath;
            baseApiUrl = baseApiUrl.TrimEnd('/');

            var scheme = ctx.Request.Scheme;
            var host = ctx.Request.Host.Value;

            // Try the current doc name first, then fall back to the legacy "v1" path.
            var urls = new[]
            {
                $"{scheme}://{host}{baseApiUrl}/openapi/jobmaster/openapi.json",
                $"{scheme}://{host}{baseApiUrl}/openapi/v1/openapi.json",
                $"{scheme}://{host}{baseApiUrl}/openapi.json"
            };

            Exception? lastEx = null;
            foreach (var jsonUrl in urls)
            {
                try
                {
                    var jsonContent = await FetchHttpAsync(jsonUrl, httpClientFactory);
                    Apply(options, jsonContent);
                    return;
                }
                catch (Exception ex)
                {
                    lastEx = ex;
                }
            }

            throw new InvalidOperationException(
                $"JobMaster Dashboard: Failed to load OpenAPI JSON spec from '{baseApiUrl}'. " +
                $"Tried: {string.Join(", ", urls)}. Last error: {lastEx?.Message}", lastEx);
        }
    }

    private static async Task<string> FetchHttpAsync(string url, IHttpClientFactory httpClientFactory)
    {
        using var client = httpClientFactory.CreateClient();
        return await client.GetStringAsync(url);
    }

    private static bool HasJsonExtension(string path) =>
        path.EndsWith(".json", StringComparison.OrdinalIgnoreCase);

    private static bool IsAbsoluteUrl(string url) =>
        url.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
        url.StartsWith("http://", StringComparison.OrdinalIgnoreCase);

    private static void Apply(DashboardOptions options, string rawContent)
    {
        using var doc = JsonDocument.Parse(rawContent);
        var root = doc.RootElement;

        if (!root.TryGetProperty("info", out var info) ||
            !info.TryGetProperty("x-jobmaster-doc", out var docExt) ||
            !string.Equals(docExt.GetString(), "JobMaster.Api.627b34633149493c9f293298ab209809", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("JobMaster Dashboard: The provided OpenAPI spec is not a valid JobMaster API document (missing or invalid 'x-jobmaster-doc' extension).");
        }

        ApplyApiUrl(options, root);
        ApplyClusters(options, root);
        ApplyAuthProviders(options, root);
    }

    private static void ApplyApiUrl(DashboardOptions options, JsonElement root)
    {
        if (!string.IsNullOrEmpty(options.ApiUrl)) return;
        if (!root.TryGetProperty("servers", out var servers)) return;
        if (servers.ValueKind != JsonValueKind.Array) return;

        foreach (var entry in servers.EnumerateArray())
        {
            var url = entry.TryGetProperty("url", out var p) ? p.GetString() : null;
            if (!string.IsNullOrWhiteSpace(url))
            {
                options.ApiUrl = url;
                return;
            }
        }
    }

    private static void ApplyClusters(DashboardOptions options, JsonElement root)
    {
        if (!root.TryGetProperty("x-jobmaster-clusters", out var clusters)) return;
        if (clusters.ValueKind != JsonValueKind.Array) return;

        foreach (var item in clusters.EnumerateArray())
        {
            var id = item.TryGetProperty("id", out var idProp) ? idProp.GetString() : null;
            if (string.IsNullOrEmpty(id)) continue;
            if (options.Clusters.Any(c => c.Id == id)) continue;

            options.Clusters.Add(new DashboardClusterConfig { Id = id, EnvironmentName = id });
        }
    }

    private static void ApplyAuthProviders(DashboardOptions options, JsonElement root)
    {
        if (!root.TryGetProperty("components", out var components)) return;
        if (!components.TryGetProperty("securitySchemes", out var schemes)) return;
        if (schemes.ValueKind != JsonValueKind.Object) return;

        foreach (var scheme in schemes.EnumerateObject())
        {
            var provider = MapScheme(scheme.Name, scheme.Value);
            if (provider is null) continue;

            // User config (including explicit disables) always wins — one provider per type.
            if (options.Auth.Providers.Any(p => p.ProviderId == provider.ProviderId)) continue;

            options.Auth.Enabled = true;
            options.Auth.Providers.Add(provider);
        }
    }

    private static DashboardAuthProviderConfig? MapScheme(string schemeName, JsonElement scheme)
    {
        if (!scheme.TryGetProperty("type", out var typeProp)) return null;

        var displayName = scheme.TryGetProperty("x-jobmaster-displayName", out var dnProp)
            ? dnProp.GetString() ?? schemeName
            : schemeName;

        switch (typeProp.GetString())
        {
            case "apiKey":
                if (!scheme.TryGetProperty("in", out var inProp) || inProp.GetString() != "header") return null;
                var header = scheme.TryGetProperty("name", out var nameProp)
                    ? nameProp.GetString() ?? "X-JobMaster-Key"
                    : "X-JobMaster-Key";
                return new ApiKeyAuthProviderConfig { DisplayName = displayName, HeaderName = header };

            case "http":
                if (!scheme.TryGetProperty("scheme", out var httpSchemeProp)) return null;
                switch (httpSchemeProp.GetString())
                {
                    case "bearer":
                        return new SimpleJwtAuthProviderConfig { DisplayName = displayName };
                    case "basic":
                        return new UserPasswordAuthProviderConfig { DisplayName = displayName };
                }
                return null;

            default:
                return null;
        }
    }
}
