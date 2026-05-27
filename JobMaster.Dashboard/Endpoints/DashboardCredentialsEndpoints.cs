using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Credentials;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Dashboard.Endpoints;

internal static class DashboardCredentialsEndpoints
{
    internal static IEndpointRouteBuilder MapDashboardCredentialsEndpoints(this IEndpointRouteBuilder endpoints, string basePath)
    {
        // Creates the session and sets the HttpOnly cookie. No credentials stored yet.
        // Rate-limited per client ID (or IP when no client ID cookie is present).
        endpoints.MapPost($"{basePath}/credentials/open-session", async (
            HttpContext ctx,
            DashboardOptions options,
            IRateSessionLimiter rateLimiter,
            CancellationToken ct) =>
        {
            var config = options.CredentialsPersistence;

            var clientId = ctx.Request.Cookies[options.ClientIdCookieName];
            var isNewClient = clientId is null;
            if (isNewClient)
                clientId = ctx.Connection.RemoteIpAddress?.ToString() ?? Guid.NewGuid().ToString("N");

            var newSessionId = Guid.NewGuid().ToString("N");
            var result = await rateLimiter.TryOpenSessionAsync(clientId, newSessionId, ct);
            if (!result.Allowed)
                return Results.StatusCode(429);

            if (isNewClient)
            {
                ctx.Response.Cookies.Append(options.ClientIdCookieName, clientId, new CookieOptions
                {
                    HttpOnly = true,
                    Secure = true,
                    SameSite = SameSiteMode.Strict,
                    MaxAge = TimeSpan.FromDays(365)
                });
            }

            AppendSessionCookie(ctx, options, newSessionId);
            return Results.Ok();
        });

        endpoints.MapPost($"{basePath}/credentials/{{credentialKey}}", async (
            [FromRoute] string credentialKey,
            HttpContext ctx,
            ICredentialsPersistenceService persistence,
            DashboardOptions options,
            [FromBody] CredentialsRequest request,
            CancellationToken ct) =>
        {
            var config = options.CredentialsPersistence;
            var sessionId = ctx.Request.Cookies[options.SessionCookieName];
            if (sessionId is null) return Results.Forbid();

            var expiry = request.DurationToExpire ?? config.DefaultCredentialsExpiry;
            var expiresAt = DateTime.UtcNow.Add(expiry);
            var stored = new StoredCredentials
            {
                Headers = request.Headers,
                ExpiresAt = expiresAt
            };

            await persistence.StoreAsync(sessionId, credentialKey, stored, ct);

            AppendSessionCookie(ctx, options, sessionId);
            return Results.Ok(expiresAt);
        });

        endpoints.MapGet($"{basePath}/credentials/{{credentialKey}}", async (
            [FromRoute] string credentialKey,
            HttpContext ctx,
            ICredentialsPersistenceService persistence,
            DashboardOptions options,
            CancellationToken ct) =>
        {
            var sessionId = ctx.Request.Cookies[options.SessionCookieName];
            if (sessionId is null) return Results.NotFound();

            var stored = await persistence.GetAsync(sessionId, credentialKey, ct);
            if (stored is null) return Results.NotFound();
            if (stored.ExpiresAt < DateTime.UtcNow) return Results.NotFound();

            AppendSessionCookie(ctx, options, sessionId);
            return Results.Ok(new CredentialsResponse { Headers = stored.Headers, ExpiryAt = stored.ExpiresAt });
        });

        // Destroys the session cookie. Stored credential entries orphan and expire via TTL.
        endpoints.MapDelete($"{basePath}/credentials/close-session", (
            HttpContext ctx,
            DashboardOptions options) =>
        {
            ctx.Response.Cookies.Delete(options.SessionCookieName);
            return Results.NoContent();
        });

        endpoints.MapDelete($"{basePath}/credentials/{{credentialKey}}", async (
            [FromRoute] string credentialKey,
            HttpContext ctx,
            ICredentialsPersistenceService persistence,
            DashboardOptions options,
            CancellationToken ct) =>
        {
            var sessionId = ctx.Request.Cookies[options.SessionCookieName];

            if (sessionId is not null)
                await persistence.RemoveAsync(sessionId, credentialKey, ct);

            return Results.NoContent();
        });

        return endpoints;
    }

    private static void AppendSessionCookie(HttpContext ctx, DashboardOptions options, string sessionId)
    {
        ctx.Response.Cookies.Append(options.SessionCookieName, sessionId, new CookieOptions
        {
            HttpOnly = true,
            Secure = true,
            SameSite = SameSiteMode.Strict,
            MaxAge = DashboardCredentialsPersistenceConfig.SessionIdleExpiry
        });
    }
}
