using JobMaster.Api.AspNetCore.Auth;
using JobMaster.Api.AspNetCore.Swagger;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace JobMaster.Api.AspNetCore;

public static class JobMasterApiWebApplicationExtensions
{
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
