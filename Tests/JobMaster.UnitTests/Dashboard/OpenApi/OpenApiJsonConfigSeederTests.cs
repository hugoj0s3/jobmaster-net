using FluentAssertions;
using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.OpenApi;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace JobMaster.UnitTests.Dashboard.OpenApi;

public class OpenApiJsonConfigSeederTests : IDisposable
{
    private readonly List<string> tempFiles = new();

    public void Dispose()
    {
        foreach (var f in tempFiles.Where(File.Exists))
            File.Delete(f);
    }

    private string WriteTempJson(string json)
    {
        var path = Path.ChangeExtension(Path.GetTempFileName(), ".json");
        File.WriteAllText(path, json);
        tempFiles.Add(path);
        return path;
    }

    private static HttpContext CreateContext()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new Mock<IWebHostEnvironment>().Object);
        services.AddHttpClient();
        return new DefaultHttpContext { RequestServices = services.BuildServiceProvider() };
    }

    private const string ValidHeader = """
        "info": { "x-jobmaster-doc": "JobMaster.Api.627b34633149493c9f293298ab209809" }
        """;

    // ── null guard ────────────────────────────────────────────────────────────

    [Fact]
    public async Task EnsureSeededAsync_WhenOpenApiUrlIsNull_DoesNothing()
    {
        var options = new DashboardOptions { OpenApiUrl = null };
        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());
        options.ApiUrl.Should().BeEmpty();
        options.Clusters.Should().BeEmpty();
    }

    // ── clusters ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task EnsureSeededAsync_WithValidFile_SetsApiUrl()
    {
        var path = WriteTempJson($$"""
            { {{ValidHeader}}, "servers": [{ "url": "/api/v1" }] }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.ApiUrl.Should().Be("/api/v1");
    }

    [Fact]
    public async Task EnsureSeededAsync_AutoAddsCluster_WithIdAsEnvironmentName()
    {
        var path = WriteTempJson($$"""
            {
              {{ValidHeader}},
              "x-jobmaster-clusters": [
                { "id": "prod" },
                { "id": "staging" }
              ]
            }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.Clusters.Should().HaveCount(2);
        options.Clusters[0].Id.Should().Be("prod");
        options.Clusters[0].EnvironmentName.Should().Be("prod");
        options.Clusters[1].Id.Should().Be("staging");
        options.Clusters[1].EnvironmentName.Should().Be("staging");
    }

    [Fact]
    public async Task EnsureSeededAsync_ManualClusterEnvironmentName_NotOverwritten()
    {
        var path = WriteTempJson($$"""
            { {{ValidHeader}}, "x-jobmaster-clusters": [{ "id": "prod" }] }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };
        options.Clusters.Add(new DashboardClusterConfig { Id = "prod", EnvironmentName = "Production" });

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.Clusters.Should().HaveCount(1);
        options.Clusters[0].EnvironmentName.Should().Be("Production");
    }

    [Fact]
    public async Task EnsureSeededAsync_DisabledCluster_NotAddedByApi()
    {
        var path = WriteTempJson($$"""
            { {{ValidHeader}}, "x-jobmaster-clusters": [{ "id": "staging" }] }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };
        options.Clusters.Add(new DashboardClusterConfig { Id = "staging", Disabled = true });

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.Clusters.Should().HaveCount(1);
        options.Clusters[0].Disabled.Should().BeTrue();
    }

    [Fact]
    public async Task EnsureSeededAsync_WithExistingApiUrl_DoesNotOverwrite()
    {
        var path = WriteTempJson($$"""
            { {{ValidHeader}}, "servers": [{ "url": "/api/new" }] }
            """);
        var options = new DashboardOptions { OpenApiUrl = path, ApiUrl = "/api/existing" };

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.ApiUrl.Should().Be("/api/existing");
    }

    [Fact]
    public async Task EnsureSeededAsync_WithDuplicateClusterId_NotAddedTwice()
    {
        var path = WriteTempJson($$"""
            {
              {{ValidHeader}},
              "x-jobmaster-clusters": [{ "id": "prod" }, { "id": "prod" }]
            }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.Clusters.Should().HaveCount(1);
    }

    // ── auth ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EnsureSeededAsync_WithValidFile_MapsAllAuthSchemes()
    {
        var path = WriteTempJson($$"""
            {
              {{ValidHeader}},
              "components": {
                "securitySchemes": {
                  "ApiKey": { "type": "apiKey", "in": "header", "name": "X-Api-Key", "x-jobmaster-displayName": "API Key" },
                  "Bearer": { "type": "http", "scheme": "bearer", "x-jobmaster-displayName": "JWT" },
                  "Basic":  { "type": "http", "scheme": "basic",  "x-jobmaster-displayName": "User/Pwd" }
                }
              }
            }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.Auth.Enabled.Should().BeTrue();
        options.Auth.Providers.Should().HaveCount(3);
        options.Auth.Providers[0].Should().BeOfType<ApiKeyAuthProviderConfig>()
            .Which.HeaderName.Should().Be("X-Api-Key");
        options.Auth.Providers[1].Should().BeOfType<SimpleJwtAuthProviderConfig>();
        options.Auth.Providers[2].Should().BeOfType<UserPasswordAuthProviderConfig>();
    }

    [Fact]
    public async Task EnsureSeededAsync_ManualAuthProvider_NotOverriddenByApi()
    {
        var path = WriteTempJson($$"""
            {
              {{ValidHeader}},
              "components": {
                "securitySchemes": {
                  "ApiKey": { "type": "apiKey", "in": "header", "name": "X-From-Api" }
                }
              }
            }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };
        options.Auth.Providers.Add(new ApiKeyAuthProviderConfig { HeaderName = "X-Manual-Key" });

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.Auth.Providers.Should().HaveCount(1);
        options.Auth.Providers[0].As<ApiKeyAuthProviderConfig>().HeaderName.Should().Be("X-Manual-Key");
    }

    [Fact]
    public async Task EnsureSeededAsync_DisabledAuthType_NotAddedByApi()
    {
        var path = WriteTempJson($$"""
            {
              {{ValidHeader}},
              "components": {
                "securitySchemes": {
                  "ApiKey": { "type": "apiKey", "in": "header", "name": "X-Api-Key" }
                }
              }
            }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };
        options.Auth.Providers.Add(new ApiKeyAuthProviderConfig { Disabled = true });

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.Auth.Providers.Should().HaveCount(1);
        options.Auth.Providers[0].Disabled.Should().BeTrue();
    }

    [Fact]
    public async Task EnsureSeededAsync_WithApiKeyInQuery_SchemeSkipped()
    {
        var path = WriteTempJson($$"""
            {
              {{ValidHeader}},
              "components": {
                "securitySchemes": {
                  "QueryKey": { "type": "apiKey", "in": "query", "name": "api_key" }
                }
              }
            }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };

        await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        options.Auth.Providers.Should().BeEmpty();
    }

    // ── error cases ───────────────────────────────────────────────────────────

    [Fact]
    public async Task EnsureSeededAsync_WithMissingDocExtension_Throws()
    {
        var path = WriteTempJson("""{ "info": {} }""");
        var options = new DashboardOptions { OpenApiUrl = path };

        var act = async () => await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*x-jobmaster-doc*");
    }

    [Fact]
    public async Task EnsureSeededAsync_WithWrongDocExtensionValue_Throws()
    {
        var path = WriteTempJson("""{ "info": { "x-jobmaster-doc": "wrong" } }""");
        var options = new DashboardOptions { OpenApiUrl = path };

        var act = async () => await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task EnsureSeededAsync_WhenFileNotFound_Throws()
    {
        var options = new DashboardOptions { OpenApiUrl = "/nonexistent/path/spec.json" };

        var act = async () => await new OpenApiJsonConfigSeeder().EnsureSeededAsync(options, CreateContext());

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*not found*");
    }

    // ── idempotency ───────────────────────────────────────────────────────────

    [Fact]
    public async Task EnsureSeededAsync_CalledTwice_OnlyAppliesOnce()
    {
        var path = WriteTempJson($$"""
            { {{ValidHeader}}, "x-jobmaster-clusters": [{ "id": "c1" }] }
            """);
        var options = new DashboardOptions { OpenApiUrl = path };
        var seeder = new OpenApiJsonConfigSeeder();

        await seeder.EnsureSeededAsync(options, CreateContext());
        await seeder.EnsureSeededAsync(options, CreateContext());

        options.Clusters.Should().HaveCount(1);
    }
}
