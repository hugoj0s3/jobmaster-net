using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Api.AspNetCore;
using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.Configurations.Themes;
using JobMaster.Dashboard.Ioc;
using JobMaster.Dashboard.Middleware;
using JobMaster.Ioc.Extensions;
using JobMaster.NatsJetStream;
using JobMaster.SampleWeb;
using JobMaster.Postgres;
using Serilog;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using Microsoft.IdentityModel.Tokens;

var builder = WebApplication.CreateBuilder(args);

if (builder.Environment.IsDevelopment())
{
    builder.Configuration.AddUserSecrets<Program>(optional: true);
}

static string ApplySecrets(string value, IConfiguration config)
{
    if (string.IsNullOrWhiteSpace(value))
    {
        return value;
    }

    var replaced = value;

    var secretTokenMatches = Regex.Matches(value, @"\bsecret_[A-Za-z0-9_]+\b");
    foreach (Match match in secretTokenMatches)
    {
        var token = match.Value;
        if (string.IsNullOrWhiteSpace(token))
        {
            continue;
        }

        var secretValue = config[token];
        if (string.IsNullOrWhiteSpace(secretValue))
        {
            continue;
        }

        replaced = replaced.Replace(token, secretValue, StringComparison.Ordinal);
    }

    var bracketTokenMatches = Regex.Matches(value, @"\[[A-Za-z0-9_]+\]");
    foreach (Match match in bracketTokenMatches)
    {
        var token = match.Value;
        if (string.IsNullOrWhiteSpace(token) || token.Length < 3)
        {
            continue;
        }

        var key = token.Substring(1, token.Length - 2);
        if (string.IsNullOrWhiteSpace(key))
        {
            continue;
        }

        var secretValue = config[key];
        if (string.IsNullOrWhiteSpace(secretValue))
        {
            continue;
        }

        replaced = replaced.Replace(token, secretValue, StringComparison.Ordinal);
    }

    return replaced;
}

var masterPostgres = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:MasterPostgres"]
    ?? "Host=[POSTGRES_HOST];Port=[POSTGRES_PORT];Database=jobmaster;Username=[POSTGRES_USER];Password=[POSTGRES_PASSWORD];Maximum Pool Size=300",
    builder.Configuration);

var natsUrl = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:NatsJetStream"]
    ?? "nats://[NATS_USER]:[NATS_PASSWORD]@[NATS_HOST]:[NATS_PORT]",
    builder.Configuration);

var standalonePostgres = ApplySecrets(
    builder.Configuration["JobMaster:SampleWeb:StandalonePostgres"]
    ?? "Host=[POSTGRES_HOST];Port=[POSTGRES_PORT];Database=jobmaster_standalone;Username=[POSTGRES_USER];Password=[POSTGRES_PASSWORD];Maximum Pool Size=300",
    builder.Configuration);

builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("Cluster-1")
          .TransientThreshold(TimeSpan.FromMinutes(1))
          .DebugJsonlFileLogger("/var/log/jobmaster/Cluster-1.log")
          .Mode(ClusterMode.Active);

    // Master database (must be SQL)
    config.UsePostgresForMaster(masterPostgres);
    
    // config.AddAgentConnectionConfig("Pg-1")
    //       .UsePostgresForAgent("Host=[POSTGRES_HOST];Port=[POSTGRES_PORT];Database=agent_pg1;Username=[POSTGRES_USER];Password=[POSTGRES_PASSWORD]");
    //
    // config.AddAgentConnectionConfig("My-1")
    //       .UseMySqlForAgent("Server=[MYSQL_HOST];Port=[MYSQL_PORT];Database=agent_my1;User ID=[MYSQL_USER];Password=[MYSQL_PASSWORD];");
    //
    // config.AddAgentConnectionConfig("Sql-1")
    //     .UseSqlServerForAgent("Server=[SQL_SERVER_HOST];Initial Catalog=agent_sql1;User Id=[SQL_SERVER_USER];Password=[SQL_SERVER_PASSWORD];Encrypt=False;TrustServerCertificate=True;");

    
    config.AddAgentConnectionConfig("Nats-1")
          .UseNatsJetStream(natsUrl);

    config.AddWorker()
        .AgentConnName("Nats-1")
        .BucketQtyConfig(JobMasterPriority.Medium, 1)
        .TransferBatchSize(1000)
        .SetWorkerMode(AgentWorkerMode.Full);

    config.AddWorker()
        .AgentConnName("Nats-1")
        .BucketQtyConfig(JobMasterPriority.Medium, 1)
        .TransferBatchSize(1000)
        .SetWorkerMode(AgentWorkerMode.Drain);
});

builder.Services.AddJobMasterCluster(c => {
        
    c.UseStandaloneCluster().ClusterId("Cluster-Standalone-1")
        .UsePostgres(standalonePostgres)
        .SetAsDefault()
        .AddWorker();
    
});

var devApiKey  = Convert.ToBase64String(RandomNumberGenerator.GetBytes(24)).Replace("+", "-").Replace("/", "_").TrimEnd('=');
var devPassword = Convert.ToBase64String(RandomNumberGenerator.GetBytes(12)).Replace("+", "-").Replace("/", "_").TrimEnd('=');
var jwtSecret  = RandomNumberGenerator.GetBytes(32);

Console.WriteLine("╔══════════════════════════════════════╗");
Console.WriteLine("║     JobMaster Dev Credentials        ║");
Console.WriteLine("╠══════════════════════════════════════╣");
Console.WriteLine($"║  API Key  : {devApiKey,-26}║");
Console.WriteLine($"║  Username : {"admin",-26}║");
Console.WriteLine($"║  Password : {devPassword,-26}║");
Console.WriteLine("╚══════════════════════════════════════╝");

var jwtTvp = new TokenValidationParameters
{
    ValidateIssuerSigningKey = true,
    IssuerSigningKey = new SymmetricSecurityKey(jwtSecret),
    ValidateIssuer = false,
    ValidateAudience = false,
    ValidateLifetime = true,
    ClockSkew = TimeSpan.Zero
};

builder.Services.UseJobMasterApi(o =>
{
    o.BasePath = "/jm-api";
    o.RequireAuthentication = true;
    o.EnableSwagger = true;
    o.UseApiKeyAuth()
        .AddApiKey("dev", devApiKey);

    o.UseUserPwdAuth()
        .AddUserPwd("admin", devPassword);

    var selector = o.UseJwtBearerAuth();
    selector.RegisterDefaultJwtBearerAuthProvider(jwtTvp);
});

// GitHub's Octicon mark, inlined as a data URI so the login button's icon doesn't depend on an
// external asset/CDN being reachable.
const string GitHubIconDataUri = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAxNiAxNiI+PHBhdGggZmlsbD0iI2ZmZmZmZiIgZD0iTTggMEMzLjU4IDAgMCAzLjU4IDAgOGMwIDMuNTQgMi4yOSA2LjUzIDUuNDcgNy41OS40LjA3LjU1LS4xNy41NS0uMzggMC0uMTktLjAxLS44Mi0uMDEtMS40OS0yLjAxLjM3LTIuNTMtLjQ5LTIuNjktLjk0LS4wOS0uMjMtLjQ4LS45NC0uODItMS4xMy0uMjgtLjE1LS42OC0uNTItLjAxLS41My42My0uMDEgMS4wOC41OCAxLjIzLjgyLjcyIDEuMjEgMS44Ny44NyAyLjMzLjY2LjA3LS41Mi4yOC0uODcuNTEtMS4wNy0xLjc4LS4yLTMuNjQtLjg5LTMuNjQtMy45NSAwLS44Ny4zMS0xLjU5LjgyLTIuMTUtLjA4LS4yLS4zNi0xLjAyLjA4LTIuMTIgMCAwIC42Ny0uMjEgMi4yLjgyLjY0LS4xOCAxLjMyLS4yNyAyLS4yNy42OCAwIDEuMzYuMDkgMiAuMjcgMS41My0xLjA0IDIuMi0uODIgMi4yLS44Mi40NCAxLjEuMTYgMS45Mi4wOCAyLjEyLjUxLjU2LjgyIDEuMjcuODIgMi4xNSAwIDMuMDctMS44NyAzLjc1LTMuNjUgMy45NS4yOS4yNS41NC43My41NCAxLjQ4IDAgMS4wNy0uMDEgMS45My0uMDEgMi4yIDAgLjIxLjE1LjQ2LjU1LjM4QTguMDEzIDguMDEzIDAgMDAxNiA4YzAtNC40Mi0zLjU4LTgtOC04eiIvPjwvc3ZnPg==";

builder.Services.AddJobMasterDashboard(dashboard =>
{
    dashboard.UseBasePath("/jm-dashboard");
    dashboard.UseApiUrl("/jm-api");
    
    dashboard.ConfigCluster("Cluster-1", "QA");
    dashboard.ConfigCluster("Cluster-Standalone-1", "DEV");
    dashboard.AddPrimaryTheme(DashboardBuiltInTheme.JobMasterLight, "JobMaster Light")
        .SetBorderRadii(box: "0.75rem", selector: "0.5rem", field: "0.5rem")
        .SetFontSans(["Inter", "system-ui", "sans-serif"])
        .SetFontMono(["JetBrains Mono", "Fira Code", "monospace"])
        .Primary("oklch(55.04% 0.184 264.09)", "oklch(97% 0.01 264)")
        .Secondary("oklch(58% 0.15 200)", "oklch(97% 0.01 200)")
        .Accent("oklch(68% 0.18 330)", "oklch(20% 0.05 330)")
        .Neutral("oklch(45% 0.02 264)", "oklch(95% 0.01 264)")
        .BaseColors("oklch(98.5% 0.002 247)", "oklch(95% 0.004 247)", "oklch(90% 0.008 247)", "oklch(22% 0.02 264)")
        .Info("oklch(62% 0.17 230)", "oklch(97% 0.01 230)")
        .Success("oklch(62% 0.17 145)", "oklch(97% 0.01 145)")
        .Warning("oklch(72% 0.18 65)", "oklch(15% 0.04 65)")
        .Error("oklch(58% 0.22 25)", "oklch(97% 0.01 25)");

    dashboard.AddTheme(DashboardBuiltInTheme.JobMasterDark, "JobMaster Dark")
        .Primary("oklch(63% 0.20 264)", "oklch(15% 0.05 264)")
        .Secondary("oklch(60% 0.16 200)", "oklch(15% 0.04 200)")
        .Accent("oklch(72% 0.18 330)", "oklch(15% 0.05 330)")
        .Neutral("oklch(35% 0.02 264)", "oklch(85% 0.01 264)")
        .BaseColors("oklch(18% 0.015 264)", "oklch(14% 0.012 264)", "oklch(11% 0.010 264)", "oklch(90% 0.015 264)")
        .Info("oklch(65% 0.17 230)", "oklch(15% 0.04 230)")
        .Success("oklch(65% 0.17 145)", "oklch(15% 0.04 145)")
        .Warning("oklch(75% 0.18 65)", "oklch(15% 0.04 65)")
        .Error("oklch(62% 0.22 25)", "oklch(15% 0.04 25)");

    dashboard.AddTheme(DashboardBuiltInTheme.Corporate, "Corporate Blue")
        .DefaultForClusterId("Cluster-1")
        .Primary("oklch(50% 0.22 230)", "oklch(97% 0.01 230)")
        .Secondary("oklch(60% 0.10 230)", "oklch(97% 0.01 230)")
        .Accent("oklch(58% 0.18 270)", "oklch(97% 0.01 270)")
        .Neutral("oklch(42% 0.02 230)", "oklch(95% 0.01 230)")
        .BaseColors("oklch(99% 0.002 230)", "oklch(96% 0.004 230)", "oklch(92% 0.006 230)", "oklch(20% 0.02 230)");

    dashboard.AddTheme(DashboardBuiltInTheme.Dark, "Midnight Pro")
        .DefaultForClusterId("Cluster-Standalone-1")
        .Primary("oklch(72% 0.17 85)", "oklch(15% 0.02 85)")
        .Secondary("oklch(55% 0.08 264)", "oklch(88% 0.01 264)")
        .Accent("oklch(72% 0.18 195)", "oklch(15% 0.04 195)")
        .Neutral("oklch(28% 0.015 280)", "oklch(85% 0.01 280)")
        .BaseColors("oklch(12% 0.010 280)", "oklch(9% 0.008 280)", "oklch(7% 0.006 280)", "oklch(88% 0.015 264)")
        .Error("oklch(62% 0.22 25)", "oklch(15% 0.04 25)");
    //
    dashboard.ConfigApiKeyAuth()
        .WithDisplayName("API Key");

    dashboard.ConfigUserPasswordAuth()
        .WithDisplayName("Username & Password");
    
    dashboard.ConfigSimpleJwtAuth()
        .WithDisplayName("Bearer Token");
    
    dashboard.ConfigJwtFormAuth("/jm-api/auth/token")
        .WithDisplayName("Login")
        .AddField("username", "UserName")
        .AddField("password", "Password", DashboardJwtFormFieldType.Password);

    dashboard.ConfigOAuth()
        .AddOAuthProvider("mock-red", "Sign in with Mock Red", "/mock-idp/mock-red/authorize", "/mock-idp/mock-red/token", "dev-client",
                backgroundColor: "#ef4444", foregroundColor: "#ffffff")
            .WithScopes("openid", "profile", "email")
        .AddOAuthProvider("mock-green", "Sign in with Mock Green", "/mock-idp/mock-green/authorize", "/mock-idp/mock-green/token", "dev-client",
                backgroundColor: "#22c55e", foregroundColor: "#ffffff")
            .WithScopes("openid", "profile", "email")
        .AddOAuthProvider("mock-blue", "Sign in with Mock Blue", "/mock-idp/mock-blue/authorize", "/mock-idp/mock-blue/token", "dev-client",
                backgroundColor: "#3b82f6", foregroundColor: "#ffffff")
            .WithScopes("openid", "profile", "email")
        .AddOAuthProvider("mock-denied", "Sign in with Mock Denied (always fails)", "/mock-idp/mock-denied/authorize", "/mock-idp/mock-denied/token", "dev-client",
                backgroundColor: "#111827", foregroundColor: "#f87171")
            .WithScopes("openid", "profile", "email")
        .WithTokenIssuer(identity =>
        {
            // Lets you exercise the rejection path (thrown from a token issuer surfaces as a
            // 403 with this message shown directly in the Login UI) without needing a real
            // business rule wired up.
            if (identity.ProviderKey == "mock-denied")
                throw new UnauthorizedAccessException("This mock provider always denies login (for testing the rejection flow).");

            return Task.FromResult(GenerateDummyJwt(identity.Subject, jwtTvp));
        })
        .WithTabLabel("Sign in with SSO");

    // Real, non-OIDC provider — exercises the UserInfoUrl fallback path (GitHub's OAuth
    // returns no id_token, unlike the mock providers above). Only added when a real GitHub
    // OAuth App's credentials are configured via user-secrets, so the sample still runs fine
    // without them.
    var githubClientId = builder.Configuration["GITHUB_CLIENT_ID"];
    var githubClientSecret = builder.Configuration["GITHUB_CLIENT_SECRET"];
    if (!string.IsNullOrEmpty(githubClientId) && !string.IsNullOrEmpty(githubClientSecret))
    {
        dashboard.ConfigOAuth()
            .AddOAuthProvider("github", "Sign in with GitHub",
                    "https://github.com/login/oauth/authorize",
                    "https://github.com/login/oauth/access_token",
                    githubClientId,
                    clientSecret: githubClientSecret,
                    scopes: new[] { "read:user" },
                    userInfoUrl: "https://api.github.com/user",
                    icon: GitHubIconDataUri,
                    backgroundColor: "#24292f", foregroundColor: "#ffffff");
    }

    dashboard.ConfigureAuthRetention()
        .SetAuthRetentionType(DashboardAuthRetentionType.ServerSideInMemory);
});

builder.Services.AddEndpointsApiExplorer();

builder.Services.AddSwaggerGen();
Log.Logger = new LoggerConfiguration()
    .WriteTo.Seq("http://localhost:5341/")
    .MinimumLevel.Debug()
    .CreateLogger();

Log.Information("Starting up");

builder.Services.AddSerilog();

if (builder.Environment.IsDevelopment())
{
    builder.Services.AddCors(options =>
    {
        options.AddDefaultPolicy(
            policy =>
            {
                policy.AllowAnyOrigin() // Allow requests from any origin
                    .AllowAnyMethod()
                    .AllowAnyHeader();
            });
    });
}

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();
app.UseHttpsRedirection();

if (app.Environment.IsDevelopment())
{
    app.UseCors();
}

app.MapJobMasterApi();
app.StartJobMasterDashboard();

app.MapPost("/jm-api/auth/token", async (HttpRequest req) =>
{
    var form = await req.ReadFormAsync();
    var username = form["username"].FirstOrDefault();
    var password = form["password"].FirstOrDefault();

    var valid = string.Equals(username, "admin", StringComparison.OrdinalIgnoreCase)
             && string.Equals(password, devPassword, StringComparison.Ordinal);

    if (!valid) return Results.Unauthorized();

    var token = GenerateDummyJwt(username!, jwtTvp);
    return Results.Ok(new { token });
})
.WithOpenApi()
.WithTags("Auth")
.WithSummary("Dummy token endpoint for dashboard login testing");

// Throwaway mock OAuth provider(s), just for exercising the dashboard's OAuth login flow
// end-to-end in dev without a real IdP. Not PKCE/state-verified server-side — it's a mock, not a
// real one. One shared route per step, parameterized by {key} so each configured mock provider
// (mock-red/mock-green/mock-blue/mock-denied) gets its own simulated identity without duplicating
// endpoints.
app.MapGet("/mock-idp/{key}/authorize", (string key, HttpRequest req) =>
{
    var redirectUri = req.Query["redirect_uri"].ToString();
    var state = req.Query["state"].ToString();
    var approveUrl = $"{redirectUri}?code=mock-code&state={Uri.EscapeDataString(state)}";
    var html = $"""
        <html><body style="font-family:sans-serif;text-align:center;margin-top:100px">
        <h2>Mock IdP — {key}</h2>
        <p>Approve login as <b>{key}-user</b> ({key}@example.com)?</p>
        <a href="{approveUrl}" style="padding:10px 20px;background:#333;color:#fff;text-decoration:none;border-radius:6px">Approve</a>
        </body></html>
        """;
    return Results.Content(html, "text/html");
})
.WithOpenApi()
.WithTags("Auth")
.WithSummary("Mock OAuth authorize endpoint for dashboard OAuth testing");

app.MapPost("/mock-idp/{key}/token", (string key) =>
{
    var idToken = GenerateMockIdToken(key, jwtTvp);
    return Results.Ok(new { access_token = $"mock-access-token-{key}", id_token = idToken, token_type = "bearer" });
})
.WithOpenApi()
.WithTags("Auth")
.WithSummary("Mock OAuth token endpoint for dashboard OAuth testing");

await app.Services.StartJobMasterRuntimeAsync();


app.MapPost("/schedule-job", async(int qty, string ? lane, string? clusterId, TimeSpan? delay, IJobMasterScheduler jobScheduler) =>
{
    if (string.IsNullOrWhiteSpace(lane)) lane = null;

    var meta = WritableMetadata.New().SetStringValue("MyMetadata", "MyValue");
    var tasks = new List<Task>();
    for (int i = 0; i < qty; i++)
    {
        var data = WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName());
        if (delay.HasValue)
            tasks.Add(jobScheduler.OnceAfterAsync<HelloJobMasterHandler>(delay.Value, data, metadata: meta, workerLane: lane, clusterId: clusterId));
        else
            tasks.Add(jobScheduler.OnceNowAsync<HelloJobMasterHandler>(data, metadata: meta, workerLane: lane, clusterId: clusterId));
    }

    await Task.WhenAll(tasks);
    
    return "Qty of scheduled jobs: " + qty;
    
}).WithOpenApi();


app.MapPost("/recurring-schedule-job", (string expressionType, string expression, string? lane, IJobMasterScheduler jobScheduler) =>
{
    jobScheduler.Recurring<HelloJobMasterHandler>(expressionType, expression, WriteableMessageData.New().SetStringValue("Name", Faker.Name.FullName()), metadata: WritableMetadata.New().SetStringValue("expression", expression), workerLane: lane);
    
}).WithOpenApi();

app.MapDelete("/cancel-recurring-schedule-job", (Guid id, IJobMasterScheduler jobScheduler) =>
{
    jobScheduler.CancelRecurring(id);
}).WithOpenApi();

app.Run();

static string GenerateDummyJwt(string username, TokenValidationParameters tvp)
{
    var handler = new JwtSecurityTokenHandler();
    var descriptor = new SecurityTokenDescriptor
    {
        Subject = new ClaimsIdentity([new Claim("sub", username)]),
        Expires = DateTime.UtcNow.AddHours(8),
        SigningCredentials = new SigningCredentials(tvp.IssuerSigningKey, SecurityAlgorithms.HmacSha256)
    };
    return handler.WriteToken(handler.CreateToken(descriptor));
}

// Simulated OIDC id_token for the mock IdP — carries an email claim (unlike GenerateDummyJwt)
// so mock OAuth logins can exercise email-based logic (e.g. a domain check in WithTokenIssuer).
static string GenerateMockIdToken(string key, TokenValidationParameters tvp)
{
    var handler = new JwtSecurityTokenHandler();
    var descriptor = new SecurityTokenDescriptor
    {
        Subject = new ClaimsIdentity([
            new Claim("sub", $"{key}-user"),
            new Claim("email", $"{key}@example.com"),
            new Claim("name", $"Mock {key}")
        ]),
        Expires = DateTime.UtcNow.AddMinutes(10),
        SigningCredentials = new SigningCredentials(tvp.IssuerSigningKey, SecurityAlgorithms.HmacSha256)
    };
    return handler.WriteToken(handler.CreateToken(descriptor));
}
