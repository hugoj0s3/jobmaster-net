> [!WARNING]
> **Experimental Alpha Release**
>
> This package is in an early stage and subject to significant changes before 1.0.
> Features and APIs may evolve, and stability is not guaranteed. Not recommended for production environments.

# JobMaster.Dashboard

A browser-based single-page application for monitoring and managing your JobMaster clusters. Served as embedded static assets directly from your ASP.NET Core application — no separate deployment required.

![JobMaster Dashboard](https://raw.githubusercontent.com/hugoj0s3/jobmaster-net/master/docs/images/dashboard-overview.png)

## 📦 Installation

```bash
dotnet add package JobMaster.Dashboard
```

> The dashboard connects to the **JobMaster API** over HTTP. If both live in the same process, also install `JobMaster.Api`. If the API is deployed separately, no additional package is needed.

## ⚙️ Setup

```csharp
// 1. Register the cluster (master DB only — no workers required for the dashboard)
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("prod");
    config.UsePostgresForMaster("Host=db.internal;Database=jobmaster;Username=app;Password=...;");
});

// 2. Configure the API
builder.Services.UseJobMasterApi(o =>
{
    o.BasePath = "/jm-api";
    o.RequireAuthentication = true;
    o.EnableSwagger = true;
    o.IncludeClusterIdsInOpenApi();
    o.UseApiKeyAuth()
     .AddApiKey("dashboard", "secure-key-here");
});

// 3. Configure the Dashboard
builder.Services.AddJobMasterDashboard(dashboard =>
{
    dashboard.UseBasePath("/jm-dashboard");
    dashboard.FromOpenApiJson(); // auto-discovers clusters and auth from the API spec

    dashboard.ConfigureAuthRetention()
        .SetAuthRetentionType(DashboardAuthRetentionType.ServerSideInMemory);

    dashboard.AddPrimaryTheme(DashboardBuiltInTheme.JobMasterLight, "Light")
        .SetBorderRadii(box: "0.75rem", selector: "0.5rem");
});

var app = builder.Build();

// 4. Start the runtime and serve the dashboard
await app.Services.StartJobMasterRuntimeAsync();
app.MapJobMasterApi();
app.StartJobMasterDashboard();

app.Run();
```

## 🛡️ Authentication

Supports **API Key**, **Username & Password**, **Simple JWT** (paste token), and **JWT Form** (login form → token endpoint). Configure which providers appear in the sign-in UI:

```csharp
dashboard.ConfigApiKeyAuth().WithDisplayName("API Key");
dashboard.ConfigUserPasswordAuth().WithDisplayName("Username & Password");
dashboard.ConfigSimpleJwtAuth().WithDisplayName("Bearer Token");
```

Or let `FromOpenApiJson()` auto-discover providers from the API's OpenAPI spec.

## 🎨 Themes & Per-Cluster Visual Identity

Pin a distinct theme to each cluster so users instantly know which environment they are in:

```csharp
dashboard.AddPrimaryTheme(DashboardBuiltInTheme.JobMasterLight, "Light");

dashboard.AddTheme(DashboardBuiltInTheme.Corporate, "Production")
    .DefaultForClusterId("prod");

dashboard.AddTheme(DashboardBuiltInTheme.Dark, "QA")
    .DefaultForClusterId("qa");
```

## 📚 Full Documentation

See [DashboardConfiguration.md](https://github.com/hugoj0s3/jobmaster-net/blob/master/docs/DashboardConfiguration.md) for the complete configuration reference.
