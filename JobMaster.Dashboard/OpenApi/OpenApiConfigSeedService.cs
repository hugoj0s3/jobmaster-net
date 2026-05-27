using System.Text.Json;
using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Auth;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;

namespace JobMaster.Dashboard.OpenApi;

internal sealed class OpenApiConfigSeedService : IHostedService
{
    private static readonly string[] DiscoveryCandidates =
    [
        "openapi.json",
        Path.Combine("swagger", "v1", "swagger.json"),
        "swagger.json"
    ];

    private readonly DashboardOptions options;
    private readonly IHttpClientFactory httpClientFactory;
    private readonly IWebHostEnvironment env;

    public OpenApiConfigSeedService(
        DashboardOptions options,
        IHttpClientFactory httpClientFactory,
        IWebHostEnvironment env)
    {
        this.options = options;
        this.httpClientFactory = httpClientFactory;
        this.env = env;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (options.OpenApiUrl is null) return Task.CompletedTask;

        return options.OpenApiUrl.Length == 0
            ? SeedFromDiscoveryAsync(cancellationToken)
            : SeedFromExplicitAsync(options.OpenApiUrl, cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task SeedFromDiscoveryAsync(CancellationToken ct)
    {
        foreach (var candidate in DiscoveryCandidates)
        {
            var fullPath = Path.Combine(env.ContentRootPath, candidate);
            if (!File.Exists(fullPath)) continue;

            var json = await File.ReadAllTextAsync(fullPath, ct);
            Apply(json);
            return;
        }

        throw new InvalidOperationException(
            $"JobMaster Dashboard: FromOpenApi() could not find an OpenAPI spec file under '{env.ContentRootPath}'. " +
            $"Tried: {string.Join(", ", DiscoveryCandidates)}. " +
            "Use FromOpenApi(path) to specify the location explicitly.");
    }

    private Task SeedFromExplicitAsync(string urlOrPath, CancellationToken ct) =>
        IsAbsoluteUrl(urlOrPath)
            ? SeedFromHttpAsync(urlOrPath, ct)
            : SeedFromFileAsync(ResolveFilePath(urlOrPath), ct);

    private async Task SeedFromHttpAsync(string url, CancellationToken ct)
    {
        var client = httpClientFactory.CreateClient();
        var json = await client.GetStringAsync(url, ct);
        Apply(json);
    }

    private async Task SeedFromFileAsync(string fullPath, CancellationToken ct)
    {
        if (!File.Exists(fullPath))
            throw new InvalidOperationException(
                $"JobMaster Dashboard: OpenAPI spec file not found at '{fullPath}'.");

        var json = await File.ReadAllTextAsync(fullPath, ct);
        Apply(json);
    }

    private void Apply(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        ApplyApiUrl(root);
        ApplyClusters(root);
        ApplyAuthProviders(root);
    }

    private void ApplyApiUrl(JsonElement root)
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

    private void ApplyClusters(JsonElement root)
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

    private void ApplyAuthProviders(JsonElement root)
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

    private string ResolveFilePath(string path) =>
        Path.IsPathRooted(path)
            ? path
            : Path.Combine(env.ContentRootPath, path.TrimStart('/', '\\', '.'));

    private static bool IsAbsoluteUrl(string url) =>
        url.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
        url.StartsWith("http://", StringComparison.OrdinalIgnoreCase);
}
