using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using JobMaster.Dashboard.Configurations;
using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.OAuthFlow;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace JobMaster.Dashboard.Endpoints;

internal static class DashboardOAuthEndpoints
{
    internal static IEndpointRouteBuilder MapDashboardOAuthEndpoints(this IEndpointRouteBuilder endpoints, string basePath)
    {
        endpoints.MapGet($"{basePath}/oauth/{{id}}", async (
            [FromRoute] string id,
            HttpContext ctx,
            DashboardOptions options,
            IJobMasterOAuthFlowStateStorage flowStorage) =>
        {
            var oauthConfig = options.Auth.Providers.OfType<OAuthAuthConfig>().FirstOrDefault();
            if (oauthConfig is null or { Disabled: true }) return Results.NotFound();

            var provider = oauthConfig.Providers.FirstOrDefault(p => p.Key == id);
            if (provider is null) return Results.NotFound();

            var codeVerifier = GenerateCodeVerifier();
            var codeChallenge = GenerateCodeChallenge(codeVerifier);
            var state = Guid.NewGuid().ToString("N");

            var flowState = new OAuthFlowState { ProviderKey = id, State = state, CodeVerifier = codeVerifier };
            var cookieValue = await flowStorage.BeginAsync(flowState);

            ctx.Response.Cookies.Append(options.OAuthFlowCookieName, cookieValue, new CookieOptions
            {
                HttpOnly = true,
                Secure = true,
                SameSite = SameSiteMode.Strict,
                MaxAge = TimeSpan.FromMinutes(10)
            });

            var redirectUri = BuildCallbackUrl(ctx, options);
            var scope = string.Join(" ", provider.Scopes);
            var url = $"{provider.AuthorizationUrl}" +
                      $"?response_type=code" +
                      $"&client_id={Uri.EscapeDataString(provider.ClientId)}" +
                      $"&redirect_uri={Uri.EscapeDataString(redirectUri)}" +
                      $"&scope={Uri.EscapeDataString(scope)}" +
                      $"&state={state}" +
                      $"&code_challenge={codeChallenge}" +
                      $"&code_challenge_method=S256";

            ctx.Response.Headers.CacheControl = "no-store";
            return Results.Ok(new { state, url });
        }).ExcludeFromDescription();

        endpoints.MapPost($"{basePath}/oauth/confirm", async (
            HttpContext ctx,
            [FromBody] OAuthConfirmRequest request,
            DashboardOptions options,
            IJobMasterOAuthFlowStateStorage flowStorage,
            IHttpClientFactory httpClientFactory,
            CancellationToken ct) =>
        {
            ctx.Response.Headers.CacheControl = "no-store";

            var cookieValue = ctx.Request.Cookies[options.OAuthFlowCookieName];
            ctx.Response.Cookies.Delete(options.OAuthFlowCookieName);
            if (cookieValue is null) return Results.BadRequest(new { error = "missing_flow" });

            var flowState = await flowStorage.ConsumeAsync(cookieValue);
            if (flowState is null) return Results.BadRequest(new { error = "invalid_or_expired_flow" });
            if (!string.IsNullOrEmpty(request.State) && request.State != flowState.State)
                return Results.BadRequest(new { error = "state_mismatch" });

            var oauthConfig = options.Auth.Providers.OfType<OAuthAuthConfig>().FirstOrDefault();
            var provider = oauthConfig?.Providers.FirstOrDefault(p => p.Key == flowState.ProviderKey);
            if (oauthConfig is null || provider is null) return Results.BadRequest(new { error = "unknown_provider" });

            var redirectUri = BuildCallbackUrl(ctx, options);
            var form = new Dictionary<string, string>
            {
                ["grant_type"] = "authorization_code",
                ["code"] = request.Code,
                ["redirect_uri"] = redirectUri,
                ["client_id"] = provider.ClientId,
                ["code_verifier"] = flowState.CodeVerifier
            };
            if (!string.IsNullOrEmpty(provider.ClientSecret)) form["client_secret"] = provider.ClientSecret;

            using var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            using var tokenRes = await client.PostAsync(ResolveUrl(ctx, provider.TokenUrl), new FormUrlEncodedContent(form), ct);
            if (!tokenRes.IsSuccessStatusCode) return Results.BadRequest(new { error = "token_exchange_failed" });
            var tokenJson = await tokenRes.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);

            OAuthUserIdentity identity;
            if (tokenJson.TryGetProperty("id_token", out var idTokenEl) && idTokenEl.GetString() is { } idToken)
            {
                identity = IdentityFromIdToken(idToken, provider.Key);
            }
            else if (!string.IsNullOrEmpty(provider.UserInfoUrl))
            {
                if (!tokenJson.TryGetProperty("access_token", out var accessTokenEl))
                    return Results.BadRequest(new { error = "no_access_token" });

                using var userInfoReq = new HttpRequestMessage(HttpMethod.Get, ResolveUrl(ctx, provider.UserInfoUrl));
                userInfoReq.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessTokenEl.GetString());
                using var userInfoRes = await client.SendAsync(userInfoReq, ct);
                if (!userInfoRes.IsSuccessStatusCode) return Results.BadRequest(new { error = "userinfo_failed" });

                var userInfoJson = await userInfoRes.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);
                identity = IdentityFromUserInfo(userInfoJson, provider.Key);
            }
            else
            {
                return Results.BadRequest(new { error = "cannot_establish_identity" });
            }

            var token = await oauthConfig.TokenIssuer!(identity);

            if (oauthConfig.OnLoginSucceeded is not null)
            {
                try { await oauthConfig.OnLoginSucceeded(identity); }
                catch { /* best-effort — a broken side effect must never fail the login */ }
            }

            return Results.Ok(new { token });
        }).ExcludeFromDescription();

        return endpoints;
    }

    private static string BuildCallbackUrl(HttpContext ctx, DashboardOptions options)
    {
        var request = ctx.Request;
        var basePath = options.BasePath?.TrimEnd('/') ?? string.Empty;
        return $"{request.Scheme}://{request.Host}{basePath}/oauth-callback/";
    }

    /// <summary>
    /// Real IdPs always configure absolute URLs. This only matters for self-hosted/mock IdPs
    /// configured with a path relative to this same host.
    /// </summary>
    private static string ResolveUrl(HttpContext ctx, string url)
    {
        if (url.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
            url.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            return url;

        return $"{ctx.Request.Scheme}://{ctx.Request.Host}{url}";
    }

    private static string GenerateCodeVerifier() => Base64UrlEncode(RandomNumberGenerator.GetBytes(32));

    private static string GenerateCodeChallenge(string verifier) =>
        Base64UrlEncode(SHA256.HashData(Encoding.ASCII.GetBytes(verifier)));

    private static string Base64UrlEncode(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static byte[] Base64UrlDecode(string input)
    {
        var s = input.Replace('-', '+').Replace('_', '/');
        s = s.PadRight(s.Length + (4 - s.Length % 4) % 4, '=');
        return Convert.FromBase64String(s);
    }

    private static OAuthUserIdentity IdentityFromIdToken(string idToken, string providerKey)
    {
        var parts = idToken.Split('.');
        var payloadJson = Encoding.UTF8.GetString(Base64UrlDecode(parts[1]));
        using var doc = JsonDocument.Parse(payloadJson);

        var claims = new Dictionary<string, string>();
        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            var value = ScalarToString(prop.Value);
            if (value is not null) claims[prop.Name] = value;
        }

        claims["provider"] = providerKey;
        var sub = claims.GetValueOrDefault("sub") ?? "unknown";
        return new OAuthUserIdentity { Subject = $"{providerKey}:{sub}", Claims = claims };
    }

    private static OAuthUserIdentity IdentityFromUserInfo(JsonElement userInfo, string providerKey)
    {
        var claims = new Dictionary<string, string>();
        foreach (var prop in userInfo.EnumerateObject())
        {
            var value = ScalarToString(prop.Value);
            if (value is not null) claims[prop.Name] = value;
        }

        claims["provider"] = providerKey;
        var id = claims.GetValueOrDefault("id") ?? claims.GetValueOrDefault("sub")
            ?? claims.GetValueOrDefault("login") ?? claims.GetValueOrDefault("email") ?? "unknown";
        return new OAuthUserIdentity { Subject = $"{providerKey}:{id}", Claims = claims };
    }

    private static string? ScalarToString(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.String => value.GetString(),
        JsonValueKind.Number => value.ToString(),
        JsonValueKind.True or JsonValueKind.False => value.ToString(),
        _ => null
    };
}

internal sealed class OAuthConfirmRequest
{
    public string Code { get; set; } = string.Empty;
    public string? State { get; set; }
}
