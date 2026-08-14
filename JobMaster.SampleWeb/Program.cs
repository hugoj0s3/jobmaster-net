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
//
// app.MapPost("/stop-immediately", async (IJob) =>
// {
//     if (JobMasterRuntime.Instance != null)
//     {
//         await JobMasterRuntime.Instance.StopImmediatelyAsync();
//     }
//     
//     return "Stop initiated";
// }).WithOpenApi();



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



// namespace JobMaster.SampleWeb
// {
//     public class HelloJobHandler : IJobMasterHandler
//     {
//         public HelloJobHandler()
//         {
//         }
//     
//         public Task HandleAsync(Job job)
//         {
//             var name = job.Data.GetStringValue("Name") ?? string.Empty;
//             SayHello(name);
//
//             RedisJobs.AddJobExecutedId(job.Id);
//             RedisJobs.RemoveScheduledJobId(job.Id);
//         
//             return Task.CompletedTask;
//         }
//
//         public static void SayHello(string name)
//         {
//             Console.WriteLine($"Hello {name}");
//         }
//     }
//
//
//
//     public class HelloJobLongRunHandler : IJobMasterHandler
//     {
//         public async Task HandleAsync(Job job)
//         {
//             for (var i = 0; i < 100; i++)
//             {
//                 await Task.Delay(i * 100);
//             }
//         
//             var name = job.Data.GetStringValue("Name");
//             Console.WriteLine($"Hello {name}");
//         }
//     }
// }
