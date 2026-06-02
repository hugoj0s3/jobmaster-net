using JobMaster.Dashboard.AuthRetention;
using JobMaster.Dashboard.Configurations;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Dashboard.Endpoints;

internal static class DashboardAuthRetentionEndpoints
{
    internal static IEndpointRouteBuilder MapDashboardAuthRetentionEndpoints(this IEndpointRouteBuilder endpoints, string basePath)
    {
        // Creates the session and sets the HttpOnly cookie. No credentials stored yet.
        endpoints.MapPost($"{basePath}/credentials/open-session", (
            HttpContext ctx,
            DashboardOptions options) =>
        {
            var newSessionId = Guid.NewGuid().ToString("N");
            AppendSessionCookie(ctx, options, newSessionId);
            return Results.Ok();
        }).ExcludeFromDescription();

        endpoints.MapPost($"{basePath}/credentials/{{credentialKey}}", async (
            [FromRoute] string credentialKey,
            HttpContext ctx,
            IAuthRetentionService persistence,
            DashboardOptions options,
            [FromBody] CredentialsRequest request,
            CancellationToken ct) =>
        {
            var config = options.AuthRetention;
            var sessionId = ctx.Request.Cookies[options.SessionCookieName];
            if (sessionId is null) return Results.Forbid();

            var expiry = request.DurationToExpire ?? config.DefaultCredentialsExpiry;
            var expiresAt = DateTime.UtcNow.Add(expiry);
            var stored = new StoredAuth
            {
                Secrets = request.Secrets,
                ExpiresAt = expiresAt
            };

            await persistence.StoreAsync(sessionId, credentialKey, stored, ct);

            AppendSessionCookie(ctx, options, sessionId);
            return Results.Ok(expiresAt);
        }).ExcludeFromDescription();

        endpoints.MapGet($"{basePath}/credentials/{{credentialKey}}", async (
            [FromRoute] string credentialKey,
            HttpContext ctx,
            IAuthRetentionService persistence,
            DashboardOptions options,
            CancellationToken ct) =>
        {
            var sessionId = ctx.Request.Cookies[options.SessionCookieName];
            if (sessionId is null) return Results.NotFound();

            var stored = await persistence.GetAsync(sessionId, credentialKey, ct);
            if (stored is null || stored.ExpiresAt < DateTime.UtcNow) return Results.NotFound();

            AppendSessionCookie(ctx, options, sessionId);
            return Results.Ok(new CredentialsResponse { Secrets = stored.Secrets, ExpiryAt = stored.ExpiresAt });
        }).ExcludeFromDescription();

        // Destroys the session cookie. Stored credential entries orphan and expire via TTL.
        endpoints.MapDelete($"{basePath}/credentials/close-session", (
            HttpContext ctx,
            DashboardOptions options) =>
        {
            ctx.Response.Cookies.Delete(options.SessionCookieName);
            return Results.NoContent();
        }).ExcludeFromDescription();

        endpoints.MapDelete($"{basePath}/credentials/{{credentialKey}}", async (
            [FromRoute] string credentialKey,
            HttpContext ctx,
            IAuthRetentionService persistence,
            DashboardOptions options,
            CancellationToken ct) =>
        {
            var sessionId = ctx.Request.Cookies[options.SessionCookieName];

            if (sessionId is not null)
                await persistence.RemoveAsync(sessionId, credentialKey, ct);

            return Results.NoContent();
        }).ExcludeFromDescription();

        return endpoints;
    }

    private static void AppendSessionCookie(HttpContext ctx, DashboardOptions options, string sessionId)
    {
        ctx.Response.Cookies.Append(options.SessionCookieName, sessionId, new CookieOptions
        {
            HttpOnly = true,
            Secure = true,
            SameSite = SameSiteMode.Strict,
            MaxAge = DashboardAuthRetentionConfig.SessionIdleExpiry
        });
    }
}
