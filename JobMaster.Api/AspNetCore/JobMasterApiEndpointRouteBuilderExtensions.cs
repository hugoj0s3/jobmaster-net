using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using JobMaster.Api.AspNetCore.Auth;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Api.AspNetCore.Internals;
using JobMaster.Api.Endpoints;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;

namespace JobMaster.Api.AspNetCore;

internal static class JobMasterApiEndpointRouteBuilderExtensions
{
    public static IEndpointRouteBuilder MapJobMasterApi(this IEndpointRouteBuilder endpoints)
    {
        var group = endpoints.GetJobMasterEndpointGroup();
        var options = endpoints.GetJobMasterApiOptions();

        group.WithGroupName($"{JobMasterApiNamespaceKey.Key}")
            .AddEndpointFilter(async (context, next) =>
            {
                var identityProvider =
                    context.HttpContext.RequestServices.GetRequiredService<IJobMasterIdentityProvider>();
                var authProvider = context.HttpContext.RequestServices
                    .GetRequiredService<IJobMasterAuthorizationProvider>();
                var identity = await identityProvider.GetIdentityAsync(context.HttpContext);

                var opt = context.HttpContext.RequestServices.GetRequiredService<IOptions<JobMasterApiOptions>>().Value;
                if (options.EnableLogging)
                {
                    LogRequest(context, identity);
                }

                if (opt.RequireAuthentication)
                {
                    if (!identity.IsAuthenticated) return Results.Unauthorized();

                    if (!await authProvider.IsAuthorizedAsync(identity, context.HttpContext))
                    {
                        return Results.Forbid();
                    }
                }

                return await next(context);
            });
        
        group.MapGet("/version", (CancellationToken ct) => 
        {
            return Results.Json(new
            {
                Product = JobMasterApiAssemblyInfo.GetServiceId(), 
                Version = JobMasterApiAssemblyInfo.GetVersion(),
            });
        });
        
        group.MapBucketsEndpoints();

        return endpoints;
    }

    private static void LogRequest(EndpointFilterInvocationContext context, JobMasterApiIdentity identity)
    {
        // 1. Try route value, then fallback to default cluster config, or skip if neither exists.
        var clusterId = context.HttpContext.GetRouteValue("cluster-id")?.ToString() 
                        ?? JobMasterClusterConnectionConfig.Default?.ClusterId;

        if (string.IsNullOrWhiteSpace(clusterId)) return;

        try 
        {
            // 2. Resolve the cluster-specific factory
            var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
            var logger = factory?.GetService<IJobMasterLogger>();

            if (logger == null) return;

            // 3. Build a structured log message
            // TraceIdentifier is perfect here for the subjectId (links the log to the specific HTTP request)
            var logMessage = 
                $"API Access | {context.HttpContext.Request.Method} {context.HttpContext.Request.Path} | Subject: {identity.Subject ?? "Anonymous"}";
            
            var subjectId = JobMasterStringUtils.SanitizeForId(context.HttpContext.TraceIdentifier);

            logger.Info(
                message: logMessage, 
                subjectType: JobMasterLogSubjectType.Api, 
                subjectId: subjectId
            );
        }
        catch 
        {
            // Silently fail logging to avoid crashing the API if a cluster factory is missing
        }
    }
}
