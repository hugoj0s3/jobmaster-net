# 🌐 JobMaster API

The `JobMaster.Api` package exposes your JobMaster clusters as a RESTful HTTP service — query jobs, inspect workers, manage recurring schedules, and stream logs, all from a single authenticated endpoint.

> **The API server only needs access to the master database.** It does not need to run workers or agents, and it does not need to share a process with the consumer/producer servers. You can deploy it as a dedicated monitoring and operations server that connects directly to the master DB over the network — completely independent from where jobs are actually processed.

---

## ⚠️ Prerequisites

1. **Master DB access**: at least one cluster master connection must be reachable from the API server.
2. **Runtime initialised**: `StartJobMasterRuntimeAsync()` must be called so the API can resolve cluster components. Without workers this is lightweight — it opens DB connections but starts no background processing.

## 📦 Installation

```bash
dotnet add package JobMaster
dotnet add package JobMaster.Api
```

## 🔗 Cluster Connection

The API connects to one or more clusters by registering their master database connections. No workers or agent connections are required — the master DB is the only dependency.

```csharp
// Single cluster
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("payroll-cluster");
    config.UsePostgresForMaster("Host=db.internal;Database=jobmaster_payroll;Username=app;Password=...;");
});
```

Multiple clusters can be registered independently. The API will serve endpoints for each of them under their own cluster ID path segment (e.g. `/jm-api/payroll-cluster/jobs`).

```csharp
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("payroll-cluster");
    config.UsePostgresForMaster(payrollConnectionString);
});

builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("notifications-cluster");
    config.UsePostgresForMaster(notificationsConnectionString);
});
```

See [ClusterConfiguration.md](ClusterConfiguration.md) and [Providers.md](Providers.md) for the full set of cluster options and supported database providers.

## ⚙️ Base Configuration

The API is configured during the service registration phase. You must call `app.MapJobMasterApi()` after building the app to map the internal routes.

```csharp
builder.Services.UseJobMasterApi(o =>
{
    o.BasePath = "/jm-api";      // Base route for all JobMaster endpoints
    o.RequireAuthentication = true; // Global toggle for security
    o.EnableSwagger = true;      // Enables isolated Swagger UI
    o.EnableLogging = true;      // Logs API requests to the Cluster Logger
});

var app = builder.Build();

// This maps the endpoints (e.g., /jm-api/my-cluster/jobs)
app.MapJobMasterApi();
```

## 🛡️ Authentication

Set `RequireAuthentication = true` in the base configuration to enforce authentication globally, then configure one of the providers below.

```csharp
builder.Services.UseJobMasterApi(o =>
{
    o.RequireAuthentication = true;
    o.UseApiKeyAuth()...  // or UseUserPwdAuth() / UseJwtBearerAuth()
});
```

### API Keys

Ideal for server-to-server communication or simple monitoring tools.

```csharp
builder.Services.UseJobMasterApi(o =>
{
    o.UseApiKeyAuth()
     .ApiKeyHeader("x-api-key") // Custom header name (Default is x-api-key)
     .AddApiKey("Grafana-Monitor", "secure-key-123")
     .AddApiKey("Admin-Tool", "another-secure-key");
});
```

### User & Password

Provides credential-based access. Passwords are encrypted using **PBKDF2 (SHA256)** with 100,000 iterations.

```csharp
builder.Services.UseJobMasterApi(o =>
{
    o.UseUserPwdAuth()
     .UserNameHeaderName("X-User-Name") // Default: X-User-Name
     .PwdHeaderName("X-Password")      // Default: X-Password
     .AddUserPwd("hugo", "p@ssword_master");
});
```

### JWT Bearer

Integrate with your existing Identity Provider (IdentityServer, Auth0, etc.) or use the built-in provider. The default provider automatically detects the key type to use the appropriate signing algorithm:

- **Symmetric Keys**: Uses HmacSha256Signature.
- **Asymmetric Keys (RSA)**: Uses RsaSha256.

```csharp
builder.Services.UseJobMasterApi(o =>
{
    o.UseJwtBearerAuth()
     .Scheme("Bearer") // Default: Bearer
     .AuthorizationHeaderName("Authorization")
     .RegisterDefaultJwtBearerAuthProvider(new TokenValidationParameters
     {
         ValidateIssuer = true,
         ValidIssuer = "https://your-auth-server.com",
         IssuerSigningKey = mySecurityKey
         // ... other standard parameters
     });
});
```

> [!TIP]
> The default provider also exposes a `GenerateToken` method via `IJobMasterJwtBearerAuthProvider`. This is perfect for issuing internal tokens without requiring an external Identity Server — the interface is automatically registered in your DI container.

### Advanced Customization

Completely replace how JobMaster identifies and authorizes requests across all mechanisms.

#### Global Identity & Authorization Overrides

- **Custom Identity**

```csharp
o.UseCustomizeJobMasterIdentityProvider<MyExternalIdProvider>();
```

- **Custom Authorization**

```csharp
o.UseCustomizeJobMasterAuthorizationProvider<MyExternalAuthorizationProvider>();
```

#### Specific Auth Type Overrides

Keep the API infrastructure but change the credential retrieval logic for a specific type:

- **RegisterApiKeyAuthProvider**: Fetch identity details from a database based on the API key.
- **RegisterUserPwdAuthProvider**: Validate credentials against your own User store.
- **RegisterJwtBearerAuthProvider**: Replace the default JWT validation logic entirely.

## 📖 Isolated Swagger UI

JobMaster API comes with a dedicated Swagger interface. It is kept separate from your main application's Swagger to prevent clutter and ensure clean security definitions.

* **URL:** `{BasePath}/swagger` (e.g., `http://localhost:5000/jm-api/swagger`)
* **Features:** Supports testing **API Key**, **Basic Auth**, and **JWT Bearer** directly from the UI.
* **Isolation:** Your host application's endpoints will not appear here, and JobMaster endpoints will not appear in your host's primary Swagger.

### Cluster Discovery via OpenAPI

Call `IncludeClusterIdsInOpenApi()` to embed all registered cluster IDs into the OpenAPI document's info extensions. This allows other tools — such as the **JobMaster Dashboard** — to discover which clusters are available without requiring separate configuration.

```csharp
builder.Services.UseJobMasterApi(o =>
{
    o.EnableSwagger = true;
    o.IncludeClusterIdsInOpenApi(); // Adds x-jobmaster-clusters to the OpenAPI info
});
```

The cluster IDs are published under the `x-jobmaster-clusters` extension key in the OpenAPI `info` object:

```json
{
  "info": {
    "x-jobmaster-clusters": ["payroll-cluster", "notifications-cluster"]
  }
}
```
