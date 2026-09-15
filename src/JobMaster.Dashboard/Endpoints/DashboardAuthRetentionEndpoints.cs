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
            IJobMasterAuthRetentionStorage persistence,
            DashboardOptions options,
            [FromBody] CredentialsRequest request) =>
        {
            var config = options.AuthRetention;
            var sessionId = ctx.Request.Cookies[options.SessionCookieName];
            // Results.Forbid() requires ASP.NET Core's own authentication middleware
            // (IAuthenticationService) to process the challenge -- not guaranteed to be
            // registered by a consuming app that only uses JobMaster's own auth abstractions
            // (e.g. jobmaster-sandbox never calls AddAuthentication()), so it throws instead of
            // ever returning 403. A missing session cookie isn't an authentication challenge to
            // begin with -- it's "no session", so a plain status code is both correct and safe.
            if (sessionId is null) return Results.StatusCode(StatusCodes.Status403Forbidden);

            var expiry = request.DurationToExpire ?? config.DefaultCredentialsExpiry;
            var expiresAt = DateTime.UtcNow.Add(expiry);
            var stored = new RetainedCredential
            {
                Secrets = request.Secrets,
                ExpiresAt = expiresAt
            };

            await persistence.StoreAsync(sessionId, credentialKey, stored);

            AppendSessionCookie(ctx, options, sessionId);
            return Results.Ok(expiresAt);
        }).ExcludeFromDescription();

        endpoints.MapGet($"{basePath}/credentials/{{credentialKey}}", async (
            [FromRoute] string credentialKey,
            HttpContext ctx,
            IJobMasterAuthRetentionStorage persistence,
            DashboardOptions options) =>
        {
            var sessionId = ctx.Request.Cookies[options.SessionCookieName];
            if (sessionId is null) return Results.NotFound();

            var stored = await persistence.GetAsync(sessionId, credentialKey);
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
            IJobMasterAuthRetentionStorage persistence,
            DashboardOptions options) =>
        {
            var sessionId = ctx.Request.Cookies[options.SessionCookieName];

            if (sessionId is not null)
                await persistence.RemoveAsync(sessionId, credentialKey);

            return Results.NoContent();
        }).ExcludeFromDescription();

        return endpoints;
    }

    private static void AppendSessionCookie(HttpContext ctx, DashboardOptions options, string sessionId)
    {
        // Secure cookies are silently dropped by every browser over plain HTTP -- hardcoding
        // Secure = true meant server-side auth retention (ServerSideInMemory/Distributed/Custom)
        // could never actually persist a session during local http://localhost development, only
        // once deployed behind real HTTPS. Tying it to the current request's own scheme means it's
        // still Secure in production (where it matters) without breaking local testing.
        ctx.Response.Cookies.Append(options.SessionCookieName, sessionId, new CookieOptions
        {
            HttpOnly = true,
            Secure = ctx.Request.IsHttps,
            SameSite = SameSiteMode.Strict,
            MaxAge = DashboardAuthRetentionConfig.SessionIdleExpiry
        });
    }
}
