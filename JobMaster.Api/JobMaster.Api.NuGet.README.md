# JobMaster.Api

The `JobMaster.Api` package transforms your orchestrator into a manageable service. It provides RESTful endpoints to monitor and manage clusters, jobs, and workers.

## 📦 Installation & NuGet
```bash
dotnet add package JobMaster
dotnet add package JobMaster.Api
```



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