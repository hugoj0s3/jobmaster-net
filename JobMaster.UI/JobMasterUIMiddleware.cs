using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;

namespace JobMaster.UI;

public class JobMasterUiMiddleware
{
    private readonly RequestDelegate _next;

    public JobMasterUiMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        if (context.Request.Path.StartsWithSegments("/job-master"))
        {
            var assembly = typeof(JobMasterUiMiddleware).Assembly;
            var resourceName = "JobMaster.UI.index.html";

            using var stream = assembly.GetManifestResourceStream(resourceName);
            using var reader = new StreamReader(stream ?? throw new InvalidOperationException("Dashboard file not found"));
            var htmlContent = await reader.ReadToEndAsync();

            context.Response.ContentType = "text/html";
            await context.Response.WriteAsync(htmlContent);
            return;
        }

        await _next(context);
    }
}

public static class JobMasterUIExtensions
{
    public static IApplicationBuilder UseJobMasterUI(this IApplicationBuilder app)
    {
        return app.UseMiddleware<JobMasterUiMiddleware>();
    }
}