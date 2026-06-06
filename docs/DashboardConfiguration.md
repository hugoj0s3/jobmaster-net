# 🖥️ JobMaster Dashboard

The `JobMaster.Dashboard` package provides a browser-based single-page application for monitoring and managing your clusters. The dashboard is configured independently using its own fluent builder, giving you full control over routing, authentication presentation, credential persistence, and visual theming.

![JobMaster Dashboard overview](img/dashboard/dashboard-overview.png)

> 📸 See [Dashboard Screenshots](DashboardScreenshots.md) for annotated screenshots of every dashboard page.

> **The dashboard connects to the JobMaster API over HTTP — it does not require the API to be in the same application.** You can host the dashboard alongside the API on the same server (using a relative URL like `/jm-api`) or point it at a remote API (using an absolute URL like `https://api.example.com/jm-api`). You only need `JobMaster.Api` installed if both live in the same process.

---

## ⚠️ Prerequisites

The dashboard is a thin UI layer on top of the JobMaster API. The API it points to must be running and accessible before the dashboard can load cluster data.

1. **API reachable**: the URL configured via `UseApiUrl` must resolve to a running JobMaster API instance.
2. **Runtime started**: the API-side runtime (`StartJobMasterRuntimeAsync`) must be active so the API can serve cluster data.

---

## 📦 Installation

```bash
dotnet add package JobMaster.Dashboard
```

---

## ⚙️ Registration

Register services with `AddJobMasterDashboard`, then start the dashboard after `builder.Build()` with a single call.

```csharp
builder.Services.AddJobMasterDashboard(dashboard =>
{
    // configure here
});

var app = builder.Build();

app.StartJobMasterDashboard();   // registers middleware + maps all dashboard endpoints
```

## 🌐 Base Path & API URL

By default the dashboard is served at `/jm-dashboard` and points to `/jm-api`. Both values can be overridden to match your deployment topology.

```csharp
builder.Services.AddJobMasterDashboard(dashboard =>
{
    dashboard.UseBasePath("/jm-dashboard");   // URL where the SPA is served
    dashboard.UseApiUrl("/jm-api");           // Base URL of the JobMaster API
});
```

`UseApiUrl` accepts a relative path (resolved against the current origin) or an absolute URL for cross-origin setups.

---

## 🗂️ Cluster Configuration

The dashboard can display one or more clusters. Each cluster entry controls the label shown in the UI and whether the cluster is visible to users.

### Registering clusters

The cluster ID must match exactly the ID configured on the API side. `environmentName` is purely informational — it appears as a badge label in the UI to help you identify what environment the cluster belongs to (e.g. Production, Staging). It is optional; omit it if you don't need a label.

```csharp
builder.Services.AddJobMasterDashboard(dashboard =>
{
    // Cluster ID must match the ID registered in the JobMaster API
    dashboard.ConfigCluster("cluster-payroll", environmentName: "Production");

    // No label — the cluster ID is shown on its own
    dashboard.ConfigCluster("cluster-payroll-staging");
});
```

### Hiding a cluster

A cluster can be hidden from the UI without removing it from the API. Both forms are equivalent:

```csharp
// Standalone helper
dashboard.DisableCluster("dev");

// Inline with ConfigCluster
dashboard.ConfigCluster("dev", disabled: true);
```

Disabled clusters are never shown in the dashboard even if the API reports them as active. This is useful for internal or maintenance clusters you do not want to expose.

---

## 🛡️ Authentication

Configure how the dashboard collects and forwards credentials to the API. Each auth type maps to a different sign-in experience in the UI — the dashboard itself does not enforce access control, it simply presents the configured options and forwards credentials on every API request.

**One provider per type** is allowed.

### API Key

Renders a single text input. The entered value is sent as the `X-Api-Key` header on every API call.

```csharp
dashboard.ConfigApiKeyAuth()
    .WithDisplayName("API Key");
```

### Username & Password

Renders a username and password form. Credentials are sent as the `X-User-Name` and `X-Password` headers on every API call.

```csharp
dashboard.ConfigUserPasswordAuth()
    .WithDisplayName("Username & Password");
```

### Simple JWT (paste token)

Renders a text area where the user pastes a pre-obtained Bearer token. The token is sent as-is in the `Authorization: Bearer` header — no token endpoint is involved.

```csharp
dashboard.ConfigSimpleJwtAuth()
    .WithDisplayName("Bearer Token");
```

### JWT Form (login form → token endpoint)

Renders a configurable login form. On submit, the dashboard posts the form fields to a token endpoint and stores the returned JWT. Use this when you have a dedicated `/auth/token` endpoint.

```csharp
dashboard.ConfigJwtFormAuth("/jm-api/auth/token")
    .WithDisplayName("Login")
    .AddField("username", "Username")
    .AddField("password", "Password", DashboardJwtFormFieldType.Password);
```

`DashboardJwtFormFieldType.Password` causes the field to render as a masked input in the UI.

---

## 🔒 Auth Retention

By default credentials are discarded on every page reload. Auth Retention controls how the dashboard persists credentials across refreshes.

```csharp
dashboard.ConfigureAuthRetention()
    .SetAuthRetentionType(DashboardAuthRetentionType.ClientSideSessionStorage);
```

| Type | Description |
|---|---|
| `NoRetention` *(default)* | Credentials are cleared on every page reload. |
| `ClientSideSessionStorage` | Credentials are encrypted and stored in the browser's Session Storage. They survive page refreshes within the same browser tab but are cleared when the tab is closed. |
| `ServerSideInMemory` | Credentials are stored in server memory. Suitable for single-instance deployments only — sessions are lost on application restart. |
| `ServerSideDistributed` | Credentials are stored via `IDistributedCache` (e.g. Redis). Suitable for multi-instance and containerised deployments. |

> [!TIP]
> `ServerSideDistributed` requires a registered `IDistributedCache` implementation in your DI container (e.g. `builder.Services.AddStackExchangeRedisCache(...)`).

---

## 🎨 Themes

The dashboard theme system is built on DaisyUI semantic color tokens and expressed in OKLCH. Every theme that is not the primary inherits the primary theme's font stack and border radii — you only need to override colors.

### Primary theme

The primary theme serves as the site-wide baseline. It is used whenever no cluster-specific theme is matched. **Font and border-radius overrides are only applied on the primary theme.**

```csharp
dashboard.AddPrimaryTheme(DashboardBuiltInTheme.JobMasterLight, "JobMaster Light")
    .SetBorderRadii(box: "0.75rem", selector: "0.5rem", field: "0.5rem")
    .SetFontSans(["Inter", "system-ui", "sans-serif"])
    .Primary("oklch(55% 0.18 264)", "oklch(97% 0.01 264)")
    .BaseColors(
        "oklch(98.5% 0.002 247)",
        "oklch(95% 0.004 247)",
        "oklch(90% 0.008 247)",
        "oklch(22% 0.02 264)"
    );
```

### Additional themes

Additional themes inherit font and border radii from the primary and only need color overrides.

```csharp
dashboard.AddTheme(DashboardBuiltInTheme.JobMasterDark, "JobMaster Dark")
    .Primary("oklch(63% 0.20 264)", "oklch(15% 0.05 264)")
    .BaseColors(
        "oklch(18% 0.005 247)",
        "oklch(22% 0.007 247)",
        "oklch(27% 0.010 247)",
        "oklch(92% 0.005 264)"
    );
```

### Assigning a theme per cluster

Pinning a distinct theme to each cluster gives users an immediate visual cue about which environment they are operating in — for example, a neutral blue for production and a warm amber for QA. This prevents costly mistakes when the dashboard is open across multiple browser tabs.

```csharp
// Neutral blue → instantly recognisable as production
dashboard.AddTheme(DashboardBuiltInTheme.Corporate, "Corporate Blue")
    .DefaultForClusterId("prod")
    .Primary("oklch(50% 0.22 230)", "oklch(97% 0.01 230)");

// Amber / warm → QA environment
dashboard.AddTheme(DashboardBuiltInTheme.Dark, "QA Amber")
    .DefaultForClusterId("qa")
    .Primary("oklch(72% 0.18 85)", "oklch(15% 0.02 85)");
```

The switch is automatic — as soon as the user navigates to a cluster the dashboard applies its assigned theme. Any cluster without an explicit assignment falls back to the primary theme.

### External fonts

`SetFontSans` accepts an optional `fontUrl` parameter. When provided, the dashboard injects a `<link rel="stylesheet">` at runtime so the font loads before it is applied. This is only effective on the primary theme.

```csharp
dashboard.AddPrimaryTheme(DashboardBuiltInTheme.JobMasterLight, "Inter Light")
    .SetFontSans(
        ["Inter", "system-ui", "sans-serif"],
        fontUrl: "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;700&display=swap"
    );
```

> [!WARNING]
> External font URLs cause the end user's browser to make a request to a third-party server. For GDPR-sensitive deployments, prefer self-hosted fonts or a privacy-respecting CDN such as [Bunny Fonts](https://fonts.bunny.net).

**Font stack rules:**
- The first entry in the array is the primary font — subsequent entries are fallbacks.
- Always end the sans stack with `"ui-sans-serif"` or `"system-ui"` and the mono stack with `"monospace"` so the browser has a safe fallback.
- Injected font URLs are deduplicated — themes that share the same URL will not insert duplicate `<link>` tags.

### Available built-in base themes

`JobMasterLight`, `JobMasterDark`, `Corporate`, `Dark`, `Light`, `Dracula`, `Synthwave`, plus any other DaisyUI built-in theme name.

---

## 📄 OpenAPI Auto-Config

`FromOpenApiJson` reads your API's OpenAPI specification and automatically fills in any clusters and auth providers not already set in your dashboard config. This is the fastest way to keep the dashboard in sync with your API's security definitions.

```csharp
// Auto-detect spec URL from UseApiUrl()
dashboard.FromOpenApiJson();

// Explicit API base URL
dashboard.FromOpenApiJson("/jm-api");

// File path (useful during development)
dashboard.FromOpenApiJson("path/to/spec.json");

// External URL
dashboard.FromOpenApiJson("https://remote/openapi.json");
```

When auth types are auto-discovered, the dashboard adds **API Key**, **Username & Password**, and **Simple JWT** for any matching security definition found in the spec. **JWT Form is never auto-added** — it requires a token endpoint URL that cannot be inferred from the spec.

Auto-config follows these rules:

- `ApiUrl` is only set from the spec if it has not already been configured via `UseApiUrl`.
- Clusters and auth providers discovered in the spec are only added if they are not already present in the dashboard config.
- Providers explicitly registered via `ConfigCluster`, `ConfigApiKeyAuth`, etc., always win over auto-discovered equivalents.
- `DisableCluster` and `DisableAuth` take full precedence — a disabled entry will never be re-added by auto-discovery.

To suppress a specific auth type from being added by auto-discovery, call `DisableAuth`:

```csharp
dashboard.DisableAuth(DashboardAuthProviderId.SimpleJwt);
```

---

## ✅ Minimal Complete Example

A self-contained setup: one cluster (master DB only — no workers needed for API + Dashboard), API Key auth, in-memory credential retention, and a primary theme. The dashboard auto-discovers clusters and auth providers from the API's OpenAPI spec via `FromOpenApiJson`.

```csharp
// 1. Register the cluster — master DB only, no workers required
builder.Services.AddJobMasterCluster(config =>
{
    config.ClusterId("prod");
    config.UsePostgresForMaster("Host=db.internal;Database=jobmaster;Username=app;Password=...;");
});

// 2. Configure the API with auth and expose cluster IDs in the OpenAPI spec
builder.Services.UseJobMasterApi(o =>
{
    o.BasePath = "/jm-api";
    o.RequireAuthentication = true;
    o.EnableSwagger = true;
    o.IncludeClusterIdsInOpenApi(); // lets the dashboard discover clusters automatically
    o.UseApiKeyAuth()
     .AddApiKey("dashboard", "secure-key-here");
});

// 3. Configure the Dashboard — auto-discovers clusters and auth from the API spec
builder.Services.AddJobMasterDashboard(dashboard =>
{
    dashboard.UseBasePath("/jm-dashboard");
    dashboard.FromOpenApiJson(); // reads /jm-api OpenAPI spec at startup

    dashboard.ConfigureAuthRetention()
        .SetAuthRetentionType(DashboardAuthRetentionType.ServerSideInMemory);

    dashboard.AddPrimaryTheme(DashboardBuiltInTheme.JobMasterLight, "Light")
        .SetBorderRadii(box: "0.75rem", selector: "0.5rem");
});

var app = builder.Build();

// 4. Start the runtime — opens DB connections, starts no background workers
await app.Services.StartJobMasterRuntimeAsync();

// 5. Map API endpoints and serve the dashboard SPA
app.MapJobMasterApi();
app.StartJobMasterDashboard();

app.Run();
```

---

## 🔗 Related Documentation

- [API Configuration](ApiConfiguration.md) — configure the API the dashboard connects to
- [Cluster Configuration](ClusterConfiguration.md) — configure the underlying JobMaster clusters
