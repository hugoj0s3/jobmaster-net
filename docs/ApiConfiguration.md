# 🌐 JobMaster API

The `JobMaster.Api` package transforms your orchestrator into a manageable service. It provides RESTful endpoints to monitor and manage clusters, jobs, and workers.

## ⚠️ Prerequisites
The API acts as a gateway to your JobMaster data. For the API to function, you must have:
1.  **A Cluster Configured**: At least one Master connection must be registered.
2.  **Runtime Started**: The JobMaster runtime must be running (`StartJobMasterRuntimeAsync`) so the API can resolve the cluster components.

## 1. Installation
Add the API package to your project:
```bash
dotnet add package JobMaster
dotnet add package JobMaster.Api
```



## ⚙️ Base Configuration
The API is configured during the service registration phase. You must call `app.StartJobMasterApi()` after building the app to map the internal routes.

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

## 🛡️ Authentication: API Keys

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

## 👤 Authentication: User & Password

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

## 🔑 Authentication: JWT Bearer

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
         ValidIssuer = "[https://your-auth-server.com](https://your-auth-server.com)",
         IssuerSigningKey = mySecurityKey
         // ... other standard parameters
     });
});
```
[!TIP] **Pro-Tip**: The default provider also includes a GenerateToken method via IJobMasterJwtBearerAuthProvider. 
                    This is perfect for generating internal tokens without requiring an external Identity Server. This interface is automatically registered in your DI container.

## 🏗️ Advanced Customization
Completely replace how JobMaster identifies and authorizes requests across all mechanisms.

### Global Identity & Authorization Overrides
- **Custom Identity**

```csharp
o.UseCustomizeJobMasterIdentityProvider<MyExternalIdProvider>();
```

- **Custom Authorization**
```csharp
o.UseCustomizeJobMasterAuthorizationProvider<MyExternalAuthorizationProvider>();
```

### Specific Auth Type Overrides
Keep the API infrastructure but change the credential retrieval logic for a specific type:
   - **RegisterApiKeyAuthProvider**: Fetch identity details from a database based on the API key.
   - **RegisterUserPwdAuthProvider**: Validate credentials against your own User store.
   - **RegisterJwtBearerAuthProvider**: Replace the default JWT validation logic entirely.

## 📖 Isolated Swagger UI

JobMaster API comes with a dedicated Swagger interface. It is kept separate from your main application's Swagger to prevent clutter and ensure clean security definitions.

* **URL:** `{BasePath}/swagger` (e.g., `http://localhost:5000/jm-api/swagger`)
* **Features:** Supports testing **API Key**, **Basic Auth**, and **JWT Bearer** directly from the UI.
* **Isolation:** Your host application's endpoints will not appear here, and JobMaster endpoints will not appear in your host's primary Swagger.