using JobMaster.Dashboard.Configurations;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;

namespace JobMaster.Dashboard.Middleware;

public static class JobMasterDashboardMiddlewareExtensions
{
    public static IApplicationBuilder UseJobMasterDashboard(this IApplicationBuilder app)
    {
        var options = app.ApplicationServices.GetService<DashboardOptions>()
            ?? new DashboardOptions();

        var basePath = options.BasePath?.TrimEnd('/') ?? string.Empty;
        if (!string.IsNullOrEmpty(basePath) && !basePath.StartsWith("/"))
        {
            basePath = "/" + basePath;
        }

        var assembly = typeof(JobMasterDashboardMiddlewareExtensions).Assembly;
        var provider = new ManifestEmbeddedFileProvider(assembly, "Embedded");

        // DIAGNOSTIC CHECK: Prevent silent 404s by validating the provider loaded correctly
        if (!provider.GetFileInfo("/index.html").Exists)
        {
            throw new FileNotFoundException(
                "CRITICAL: '/index.html' was not found in the embedded files! " + 
                "Check your .csproj <EmbeddedResource> mapping. The Svelte output might be incorrectly nested inside a 'build/' or 'dist/' folder, or the GenerateEmbeddedFilesManifest flag is missing.");
        }

        var indexHtml = ReadIndexWithInjectedConfig(provider, basePath);

        // Rewrite exact matches and SPA routes to index.html so UseStaticFiles can serve the entry point.
        app.Use(async (ctx, next) =>
        {
            var path = ctx.Request.Path.Value ?? string.Empty;

            var routePrefix = string.IsNullOrEmpty(basePath) ? "/" : basePath + "/";
            var rootMatch = string.IsNullOrEmpty(basePath) ? "/" : basePath;

            bool isExactMatch = string.Equals(path, rootMatch, StringComparison.OrdinalIgnoreCase) ||
                                string.Equals(path, routePrefix, StringComparison.OrdinalIgnoreCase);

            if (isExactMatch)
            {
                ctx.Response.StatusCode = 200;
                ctx.Response.ContentType = "text/html; charset=utf-8";
                await ctx.Response.WriteAsync(indexHtml);
                return;
            }

            // Allow SPA client-side routing by fallback to index.html
            if (path.StartsWith(routePrefix, StringComparison.OrdinalIgnoreCase))
            {
                var relativePath = path.Substring(basePath.Length);
                if (!provider.GetFileInfo(relativePath).Exists)
                {
                    var trimmedPath = path.TrimEnd('/');
                    bool hasExtension = System.IO.Path.HasExtension(trimmedPath);
                    bool acceptsHtml = ctx.Request.Headers.Accept.ToString().Contains("text/html", StringComparison.OrdinalIgnoreCase);
                    if (!hasExtension && acceptsHtml)
                    {
                        ctx.Response.StatusCode = 200;
                        ctx.Response.ContentType = "text/html; charset=utf-8";
                        await ctx.Response.WriteAsync(indexHtml);
                        return;
                    }
                }
            }

            await next();
        });

        // Serve dashboard files under /jm-dashboard/
        app.UseStaticFiles(new StaticFileOptions
        {
            RequestPath = basePath,
            FileProvider = provider
        });

        // SvelteKit emits absolute /_app/... hrefs regardless of base path.
        // Scope this fallback STRICTLY to /_app so we don't accidentally serve 
        // the dashboard's index.html at the root of the hosting application.
        app.UseWhen(ctx => ctx.Request.Path.StartsWithSegments("/_app"), appBuilder =>
        {
            appBuilder.UseStaticFiles(new StaticFileOptions { FileProvider = provider });
        });

        return app;
    }

    private static string ReadIndexWithInjectedConfig(ManifestEmbeddedFileProvider provider, string basePath)
    {
        using var stream = provider.GetFileInfo("/index.html").CreateReadStream();
        using var reader = new System.IO.StreamReader(stream);
        var html = reader.ReadToEnd();

        var json = System.Text.Json.JsonSerializer.Serialize(new { basePath });
        var script = $"<script>window.__JM__={json};</script>";
        return html.Replace("<!-- __JM_RUNTIME_CONFIG__ -->", script);
    }
}
