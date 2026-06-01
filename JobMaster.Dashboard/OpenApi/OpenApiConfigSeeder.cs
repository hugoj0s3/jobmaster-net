using System.Text.Json;
using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Auth;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Dashboard.OpenApi;

internal static class OpenApiConfigSeeder
{
    private static int isSeeded = 0;
    private static readonly SemaphoreSlim seederLock = new(1, 1);

    public static async Task EnsureSeededAsync(DashboardOptions options, HttpContext ctx)
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
            // The Url is external - only accepts full path.
            var json = await FetchHttpAsync(rawUrlOrPath, httpClientFactory);
            Apply(options, json);
        }
        else
        {
            // The url is internal:
            if (HasFileExtension(rawUrlOrPath))
            {
                // It is pointing to the json or yaml like full path read this file.
                var env = ctx.RequestServices.GetRequiredService<IWebHostEnvironment>();
                var fullPath = Path.IsPathRooted(rawUrlOrPath)
                    ? rawUrlOrPath
                    : Path.Combine(env.ContentRootPath, rawUrlOrPath.TrimStart('/', '\\', '.'));

                if (!File.Exists(fullPath))
                    throw new InvalidOperationException($"JobMaster Dashboard: OpenAPI spec file not found at '{fullPath}'.");

                var content = await File.ReadAllTextAsync(fullPath);
                Apply(options, content);
            }
            else
            {
                // It is not full path assume it is the base url append default. try json and then try yaml.
                var baseApiUrl = string.IsNullOrEmpty(rawUrlOrPath) ? options.ApiUrl : rawUrlOrPath;
                baseApiUrl = baseApiUrl.TrimEnd('/');

                var scheme = ctx.Request.Scheme;
                var host = ctx.Request.Host.Value;

                // Try JSON first: /openapi/v1/openapi.json
                var jsonUrl = $"{scheme}://{host}{baseApiUrl}/openapi/v1/openapi.json";
                try
                {
                    var jsonContent = await FetchHttpAsync(jsonUrl, httpClientFactory);
                    Apply(options, jsonContent);
                    return;
                }
                catch (Exception ex)
                {
                    // If JSON fails, try YAML: /openapi/v1/openapi.yaml
                    var yamlUrl = $"{scheme}://{host}{baseApiUrl}/openapi/v1/openapi.yaml";
                    try
                    {
                        var yamlContent = await FetchHttpAsync(yamlUrl, httpClientFactory);
                        Apply(options, yamlContent);
                        return;
                    }
                    catch (Exception exYaml)
                    {
                        throw new InvalidOperationException(
                            $"JobMaster Dashboard: Failed to load OpenAPI spec from internal base API URL '{baseApiUrl}'. " +
                            $"Tried JSON ({jsonUrl}): {ex.Message}. " +
                            $"Tried YAML ({yamlUrl}): {exYaml.Message}", exYaml);
                    }
                }
            }
        }
    }

    private static async Task<string> FetchHttpAsync(string url, IHttpClientFactory httpClientFactory)
    {
        using var client = httpClientFactory.CreateClient();
        return await client.GetStringAsync(url);
    }

    private static bool HasFileExtension(string path) =>
        path.EndsWith(".json", StringComparison.OrdinalIgnoreCase) ||
        path.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase) ||
        path.EndsWith(".yml", StringComparison.OrdinalIgnoreCase);

    private static bool IsAbsoluteUrl(string url) =>
        url.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
        url.StartsWith("http://", StringComparison.OrdinalIgnoreCase);

    private static void Apply(DashboardOptions options, string rawContent)
    {
        using var doc = JsonDocument.Parse(rawContent);
        var root = doc.RootElement;

        // Namespace check: Verify it is a valid JobMaster API document
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

            var environmentName = item.TryGetProperty("environmentName", out var envProp)
                ? envProp.GetString() ?? id
                : id;

            options.Clusters.Add(new DashboardClusterConfig { Id = id, EnvironmentName = environmentName });
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
