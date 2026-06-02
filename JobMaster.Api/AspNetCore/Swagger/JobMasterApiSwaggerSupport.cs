using JobMaster.Api.AspNetCore.Auth;
using JobMaster.Api.AspNetCore.Internals;
using JobMaster.Sdk.Abstractions.Config;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Interfaces;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace JobMaster.Api.AspNetCore.Swagger;

internal static class JobMasterApiSwaggerSupport
{
    /// <summary>
    /// Swagger document name and endpoint group tag used to isolate JobMaster API endpoints
    /// from the host application's own swagger documents.
    /// The OpenAPI JSON is served at <c>{basePath}/openapi/{DocName}/openapi.json</c>.
    /// <para>
    /// <b>Future:</b> will be configurable via <c>JobMasterApiOptions</c>. The dashboard's
    /// <c>FromOpenApiJson</c> builder method will expose a matching <c>docName</c> parameter
    /// so both sides stay aligned without hardcoding.
    /// </para>
    /// </summary>
    internal const string DocName = "jobmaster";

    public static void ConfigureServices(IServiceCollection services, JobMasterApiOptions options)
    {
        if (!options.EnableSwagger) return;

        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen();

        // PostConfigure runs AFTER the host application's AddSwaggerGen setup.
        services.PostConfigure<SwaggerGenOptions>(opt =>
        {
            // Ensure there is at least one host doc so Swashbuckle does not error.
            if (opt.SwaggerGeneratorOptions.SwaggerDocs.Count == 0)
                opt.SwaggerDoc("v1", new OpenApiInfo());

            // JobMaster document — named with the unique namespace key so it never
            // conflicts with any document the host application defines.
            opt.SwaggerDoc(DocName, new OpenApiInfo
            {
                Title = JobMasterApiAssemblyInfo.GetServiceId(),
                Version = JobMasterApiAssemblyInfo.GetVersion(),
                Extensions = new Dictionary<string, IOpenApiExtension>
                {
                    ["x-jobmaster-doc"] = new OpenApiString(DocName)
                }
            });

            // Isolate endpoints: JobMaster endpoints go only into the JobMaster doc,
            // host endpoints are hidden from it.
            var previous = opt.SwaggerGeneratorOptions.DocInclusionPredicate;
            opt.SwaggerGeneratorOptions.DocInclusionPredicate = (doc, apiDesc) =>
            {
                var isJmDoc      = string.Equals(doc, DocName, StringComparison.OrdinalIgnoreCase);
                var isJmEndpoint = string.Equals(apiDesc.GroupName, DocName, StringComparison.OrdinalIgnoreCase);

                if (isJmDoc)      return isJmEndpoint;
                if (isJmEndpoint) return false;

                return previous?.Invoke(doc, apiDesc) ?? true;
            };

            opt.DocumentFilter<JobMasterApiSecurityDocumentFilter>();
        });
    }

    public static void ConfigureApplication(WebApplication app, JobMasterApiOptions options)
    {
        if (!options.EnableSwagger) return;

        var basePath = JobMasterApiPath.NormalizeBasePath(options.BasePath).TrimStart('/');

        // Serve swagger JSON under the API base path so it doesn't share the
        // default /swagger route with the host application.
        app.UseSwagger(c =>
        {
            c.RouteTemplate = $"{basePath}/openapi/{{documentName}}/openapi.json";
        });

        app.UseSwaggerUI(c =>
        {
            c.RoutePrefix = $"{basePath}/swagger";
            c.SwaggerEndpoint($"/{basePath}/openapi/{DocName}/openapi.json", "JobMaster.Api");

            // Include any host application swagger docs in the same UI.
            var swaggerOptions = app.Services.GetService<IOptions<SwaggerGenOptions>>();
            if (swaggerOptions != null)
            {
                foreach (var (name, info) in swaggerOptions.Value.SwaggerGeneratorOptions.SwaggerDocs)
                {
                    if (string.Equals(name, DocName, StringComparison.OrdinalIgnoreCase)) continue;
                    c.SwaggerEndpoint($"/swagger/{name}/swagger.json", info.Title ?? name);
                }
            }
        });
    }
}

/// <summary>
/// Document filter that applies security schemes and cluster metadata only to the JobMaster document.
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
        if (swaggerDoc.Info.Extensions == null ||
            !swaggerDoc.Info.Extensions.TryGetValue("x-jobmaster-doc", out var ext) ||
            ext is not OpenApiString extStr ||
            !string.Equals(extStr.Value, $"{JobMasterApiNamespaceKey.Key}", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        ApplySecuritySchemes(swaggerDoc);
        ApplyClusterIds(swaggerDoc);
    }

    private void ApplySecuritySchemes(OpenApiDocument swaggerDoc)
    {
        var supported = jobMasterOptions.Value.GetAuthenticationTypesSupported();
        if (supported.Count == 0) return;

        swaggerDoc.Components ??= new OpenApiComponents();
        swaggerDoc.Components.SecuritySchemes ??= new Dictionary<string, OpenApiSecurityScheme>();
        swaggerDoc.SecurityRequirements ??= new List<OpenApiSecurityRequirement>();

        var requirement = new OpenApiSecurityRequirement();

        if (supported.Contains(JobMasterApiAuthenticationType.ApiKey))
        {
            var header = jobMasterOptions.Value.ApiKeyOptions?.ApiKeyHeader ?? "X-Api-Key";
            const string id = "JobMasterApiKey";
            swaggerDoc.Components.SecuritySchemes[id] = new OpenApiSecurityScheme
            {
                Type = SecuritySchemeType.ApiKey, In = ParameterLocation.Header, Name = header,
                Description = "JobMaster API Key authentication"
            };
            requirement[new OpenApiSecurityScheme { Reference = new OpenApiReference { Type = ReferenceType.SecurityScheme, Id = id } }] = Array.Empty<string>();
        }

        if (supported.Contains(JobMasterApiAuthenticationType.JwtBearer))
        {
            const string id = "JobMasterBearer";
            var headerName = jobMasterOptions.Value.JwtBearerOptions?.AuthorizationHeaderName;
            if (string.IsNullOrWhiteSpace(headerName)) headerName = "Authorization";

            var configuredScheme = jobMasterOptions.Value.JwtBearerOptions?.Scheme;
            var scheme = string.IsNullOrWhiteSpace(configuredScheme) ? "Bearer" : configuredScheme;

            swaggerDoc.Components.SecuritySchemes[id] = string.Equals(scheme, "Bearer", StringComparison.OrdinalIgnoreCase)
                ? new OpenApiSecurityScheme
                {
                    Type = SecuritySchemeType.Http, Scheme = "bearer", BearerFormat = "JWT",
                    Description = "JWT Bearer authentication."
                }
                : new OpenApiSecurityScheme
                {
                    Type = SecuritySchemeType.ApiKey, In = ParameterLocation.Header, Name = headerName,
                    Description = $"JWT authentication via header '{headerName}'."
                };

            requirement[new OpenApiSecurityScheme { Reference = new OpenApiReference { Type = ReferenceType.SecurityScheme, Id = id } }] = Array.Empty<string>();
        }

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
            swaggerDoc.SecurityRequirements.Add(requirement);
    }

    private void ApplyClusterIds(OpenApiDocument swaggerDoc)
    {
        if (!jobMasterOptions.Value.IncludeClusterIdsInOpenApiDoc) return;

        var ids = JobMasterClusterConnectionConfig.GetAllConfigs()
            .Select(x => x.ClusterId)
            .ToList();

        var array = new OpenApiArray();
        foreach (var id in ids)
            array.Add(new OpenApiString(id));

        swaggerDoc.Info.Extensions["x-jobmaster-clusters"] = array;
    }
}