using System.IdentityModel.Tokens.Jwt;
using System.Reflection;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text.Json;
using JobMaster.Api.AspNetCore;
using JobMaster.Ioc.Extensions;
using Microsoft.IdentityModel.Tokens;

// ConfigFromJson only sets RepoType as a string — it never calls a provider-specific method
// directly, so the CLR would never actually load these assemblies into the AppDomain, and their
// [JobMasterIocRegistration] providers (which register IMasterGenericRecordRepository etc.) would
// be invisible to the AppDomain-based reflection scan in
// JobMasterIocRegistrationAttribute.GetRegistrationTypes(). A `typeof(...)` reference alone is not
// enough to guarantee this (the JIT can elide an unused ldtoken), so load them explicitly by path.
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.Postgres.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.MySql.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.SqlServer.dll"));
Assembly.LoadFrom(Path.Combine(AppContext.BaseDirectory, "JobMaster.RavenDb.dll"));

var builder = WebApplication.CreateBuilder(args);

var clusterConfigsJson = Environment.GetEnvironmentVariable("JOBMASTER_CLUSTER_CONFIGS_JSON");
if (string.IsNullOrWhiteSpace(clusterConfigsJson))
{
    throw new InvalidOperationException("JOBMASTER_CLUSTER_CONFIGS_JSON environment variable must be set to a JSON array of cluster configs.");
}

var clusterConfigs = JsonSerializer.Deserialize<JsonElement[]>(clusterConfigsJson)
    ?? throw new InvalidOperationException("JOBMASTER_CLUSTER_CONFIGS_JSON did not deserialize to a JSON array.");

foreach (var clusterConfig in clusterConfigs)
{
    var json = clusterConfig.GetRawText();
    builder.Services.AddJobMasterCluster(c => c.ConfigFromJson(json));
}

var requireAuth = string.Equals(
    Environment.GetEnvironmentVariable("JOBMASTER_API_REQUIRE_AUTH"),
    "true",
    StringComparison.OrdinalIgnoreCase);

var apiKey = Environment.GetEnvironmentVariable("JOBMASTER_API_KEY");
var apiUsername = Environment.GetEnvironmentVariable("JOBMASTER_API_USERNAME");
var apiPassword = Environment.GetEnvironmentVariable("JOBMASTER_API_PASSWORD");
var basePath = Environment.GetEnvironmentVariable("JOBMASTER_API_BASE_PATH");

var enableJwt = string.Equals(
    Environment.GetEnvironmentVariable("JOBMASTER_API_ENABLE_JWT"),
    "true",
    StringComparison.OrdinalIgnoreCase);

// Signing key is generated per-container-instance — the test never needs to know it, only the
// /auth/token endpoint below (which signs with it) and the JWT validation wired into
// UseJobMasterApi (which validates with it) need access.
var jwtSigningKey = RandomNumberGenerator.GetBytes(32);
var jwtTvp = new TokenValidationParameters
{
    ValidateIssuerSigningKey = true,
    IssuerSigningKey = new SymmetricSecurityKey(jwtSigningKey),
    ValidateIssuer = false,
    ValidateAudience = false,
    ValidateLifetime = true,
    ClockSkew = TimeSpan.Zero
};

builder.Services.UseJobMasterApi(o =>
{
    o.BasePath = string.IsNullOrWhiteSpace(basePath) ? "/jm-api" : basePath;

    if (!string.IsNullOrWhiteSpace(apiKey))
    {
        o.UseApiKeyAuth().AddApiKey("scenario-test", apiKey);
    }

    if (!string.IsNullOrWhiteSpace(apiUsername) && !string.IsNullOrWhiteSpace(apiPassword))
    {
        o.UseUserPwdAuth().AddUserPwd(apiUsername, apiPassword);
    }

    if (enableJwt)
    {
        o.UseJwtBearerAuth().RegisterDefaultJwtBearerAuthProvider(jwtTvp);
    }

    // Explicit last: honors the scenario's flag even when it disagrees with the auto-enable that
    // UseApiKeyAuth()/UseUserPwdAuth()/UseJwtBearerAuth() perform (e.g. a scenario configuring
    // credentials without enforcing them yet).
    o.RequireAuthentication = requireAuth;
});

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.MapJobMasterApi();

// Test-only mechanism: mints a JWT signed with this container's own signing key, so a scenario
// test can fetch a token here and present it as a Bearer credential against the jm-api routes
// above. Deliberately unauthenticated and outside the jm-api group — it exists purely to make the
// JWT auth path exercisable from a scenario test, not as part of JobMaster's real API surface.
app.MapPost("/auth/token", (JwtTokenRequest req) =>
{
    var handler = new JwtSecurityTokenHandler();
    var descriptor = new SecurityTokenDescriptor
    {
        Subject = new ClaimsIdentity([new Claim("sub", req.Subject)]),
        Expires = DateTime.UtcNow.AddHours(1),
        SigningCredentials = new SigningCredentials(jwtTvp.IssuerSigningKey, SecurityAlgorithms.HmacSha256)
    };

    var token = handler.WriteToken(handler.CreateToken(descriptor));
    return Results.Ok(new JwtTokenResponse(token));
});

await app.Services.StartJobMasterRuntimeAsync();

app.Run();

internal sealed record JwtTokenRequest(string Subject);

internal sealed record JwtTokenResponse(string Token);
