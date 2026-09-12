using JobMaster.Api.AspNetCore.Auth;
using JobMaster.Api.AspNetCore.Swagger;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace JobMaster.Api.AspNetCore;

/// <summary>Extension methods for mapping the JobMaster API endpoints onto an ASP.NET Core <see cref="WebApplication"/>.</summary>
public static class JobMasterApiWebApplicationExtensions
{
    /// <summary>Maps all JobMaster API endpoints and configures Swagger if enabled.</summary>
    public static WebApplication MapJobMasterApi(this WebApplication app)
    {
        if (app == null) throw new ArgumentNullException(nameof(app));

        var options = app.Services.GetService<IOptions<JobMasterApiOptions>>()?.Value
                      ?? new JobMasterApiOptions();

        if (options.EnableSwagger)
        {
            JobMasterApiSwaggerSupport.ConfigureApplication(app, options);
        }

        JobMasterApiEndpointRouteBuilderExtensions.MapJobMasterApi(app);

        return app;
     }
 }
