using JobMaster.Api.AspNetCore.Auth;
using JobMaster.Api.AspNetCore.Internals;
using JobMaster.Sdk.Abstractions.Keys;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Interfaces;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;
using Swashbuckle.AspNetCore.SwaggerUI;

namespace JobMaster.Api.AspNetCore.Swagger;

internal static class JobMasterApiSwaggerSupport
{
    public static void ConfigureServices(IServiceCollection services, JobMasterApiOptions options)
    {
        if (!options.EnableSwagger) return;

        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen();

        // Use PostConfigure to ensure this runs AFTER the host application's setup
        services.PostConfigure<SwaggerGenOptions>(opt =>
        {
            // Swashbuckle requires at least one SwaggerDoc. If the host didn't register any,
            // add a minimal default so /swagger doesn't break.
            if (opt.SwaggerGeneratorOptions.SwaggerDocs.Count == 0)
            {
                opt.SwaggerDoc("v1", new OpenApiInfo());
            }

            // 1. Define the JobMaster Document
            opt.SwaggerDoc($"{JobMasterApiNamespaceKey.Key}", new OpenApiInfo
            {
                Title = JobMasterApiAssemblyInfo.GetServiceId(),
                Version = JobMasterApiAssemblyInfo.GetVersion(),
                Extensions = new Dictionary<string, IOpenApiExtension>
                {
                    ["x-jobmaster-doc"] = new OpenApiString($"{JobMasterApiNamespaceKey.Key}")
                }
            });

            // 2. Isolate Endpoints
            var previousPredicate = opt.SwaggerGeneratorOptions.DocInclusionPredicate;
            opt.SwaggerGeneratorOptions.DocInclusionPredicate = (docName, apiDesc) =>
            {
                var isJmDoc = string.Equals(docName, $"{JobMasterApiNamespaceKey.Key}", StringComparison.OrdinalIgnoreCase);
                var isJmEndpoint = string.Equals(apiDesc.GroupName, $"{JobMasterApiNamespaceKey.Key}", StringComparison.OrdinalIgnoreCase);

                if (isJmDoc) return isJmEndpoint;
                if (isJmEndpoint) return false; // Hide JM from host docs

                return previousPredicate?.Invoke(docName, apiDesc) ?? true;
            };

            // 3. Add Security Filter
            opt.DocumentFilter<JobMasterApiSecurityDocumentFilter>();
        });
    }

    public static void ConfigureApplication(WebApplication app, JobMasterApiOptions options)
    {
        if (!options.EnableSwagger) return;

        // ONLY use the default JSON endpoint
        app.UseSwagger();

        // Dedicated UI for JobMaster
        app.UseSwaggerUI(c =>
        {
            var basePath = JobMasterApiPath.NormalizeBasePath(options.BasePath);
            c.RoutePrefix = $"{basePath.TrimStart('/')}/swagger";
            c.SwaggerEndpoint($"/swagger/{JobMasterApiNamespaceKey.Key}/swagger.json", "JobMaster.Api");
        });
    }
}

/// <summary>
/// Filter that applies security schemes (ApiKey, JWT, Basic) only to the JobMaster document.
/// </summary>
internal sealed class JobMasterApiSecurityDocumentFilter : IDocumentFilter
{
    private readonly IOptions<JobMasterApiOptions> jobMasterOptions;

    public JobMasterApiSecurityDocumentFilter(IOptions<JobMasterApiOptions> jobMasterOptions)
    {
        this.jobMasterOptions = jobMasterOptions;
    }

    public void Apply(OpenApiDocument swaggerDoc, DocumentFilterContext context)
    {
        // Only proceed if this is the JobMaster document
        if (swaggerDoc.Info?.Extensions == null || 
            !swaggerDoc.Info.Extensions.TryGetValue("x-jobmaster-doc", out var ext) ||
            !(ext is OpenApiString extStr) ||
            !string.Equals(extStr.Value, JobMasterApiNamespaceKey.Key.ToString(), StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var supported = jobMasterOptions.Value?.GetAuthenticationTypesSupported() ?? Array.Empty<JobMasterApiAuthenticationType>();
        if (supported.Count == 0) return;

        swaggerDoc.Components ??= new OpenApiComponents();
        swaggerDoc.Components.SecuritySchemes ??= new Dictionary<string, OpenApiSecurityScheme>();
        swaggerDoc.SecurityRequirements ??= new List<OpenApiSecurityRequirement>();

        var requirement = new OpenApiSecurityRequirement();

        // Configure ApiKey Security
        if (supported.Contains(JobMasterApiAuthenticationType.ApiKey))
        {
            var header = jobMasterOptions.Value?.ApiKeyOptions?.ApiKeyHeader ?? "api-key";
            const string id = "JobMasterApiKey";
            swaggerDoc.Components.SecuritySchemes[id] = new OpenApiSecurityScheme
            {
                Type = SecuritySchemeType.ApiKey, In = ParameterLocation.Header, Name = header,
                Description = "JobMaster API Key authentication"
            };
            requirement[new OpenApiSecurityScheme { Reference = new OpenApiReference { Type = ReferenceType.SecurityScheme, Id = id } }] = Array.Empty<string>();
        }

        // Configure JWT Security
        if (supported.Contains(JobMasterApiAuthenticationType.JwtBearer))
        {
            const string id = "JobMasterBearer";

            var headerName = jobMasterOptions.Value?.JwtBearerOptions?.AuthorizationHeaderName;
            if (string.IsNullOrWhiteSpace(headerName))
            {
                headerName = "Authorization";
            }

            var configuredScheme = jobMasterOptions.Value?.JwtBearerOptions?.Scheme;
            var scheme = string.IsNullOrWhiteSpace(configuredScheme) ? "Bearer" : configuredScheme;

            if (string.Equals(scheme, "Bearer", StringComparison.OrdinalIgnoreCase))
            {
                swaggerDoc.Components.SecuritySchemes[id] = new OpenApiSecurityScheme
                {
                    Type = SecuritySchemeType.Http,
                    Scheme = "bearer",
                    BearerFormat = "JWT",
                    Description = "JWT Bearer authentication. Example: 'Authorization: Bearer {token}'.",
                };
            }
            else
            {
                swaggerDoc.Components.SecuritySchemes[id] = new OpenApiSecurityScheme
                {
                    Type = SecuritySchemeType.ApiKey,
                    In = ParameterLocation.Header,
                    Name = headerName,
                    Description = $"JWT authentication via header '{headerName}'. Example: '{scheme} {{token}}'.",
                };
            }

            requirement[new OpenApiSecurityScheme { Reference = new OpenApiReference { Type = ReferenceType.SecurityScheme, Id = id } }] = Array.Empty<string>();
        }

        // Configure Basic Auth (User/Password)
        if (supported.Contains(JobMasterApiAuthenticationType.UserPwd))
        {
            const string id = "JobMasterBasic";
            swaggerDoc.Components.SecuritySchemes[id] = new OpenApiSecurityScheme
            {
                Type = SecuritySchemeType.Http, Scheme = "basic",
                Description = "JobMaster User/Password authentication"
            };
            requirement[new OpenApiSecurityScheme { Reference = new OpenApiReference { Type = ReferenceType.SecurityScheme, Id = id } }] = Array.Empty<string>();
        }

        if (requirement.Count > 0)
        {
            swaggerDoc.SecurityRequirements.Add(requirement);
        }
    }
}