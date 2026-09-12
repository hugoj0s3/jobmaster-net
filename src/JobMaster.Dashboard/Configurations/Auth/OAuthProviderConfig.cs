namespace JobMaster.Dashboard.Configurations.Auth;

/// <summary>
/// Configuration for a single OAuth identity provider (e.g. "github", "google"), nested under
/// the single <see cref="OAuthAuthConfig"/>. Populated by <c>AddOAuthProvider(...)</c>'s
/// parameters and its returned selector's <c>With...()</c> methods — never constructed directly
/// by callers, so this stays internal like every other auth type's config.
/// </summary>
internal sealed class OAuthProviderConfig
{
    /// <summary>
    /// Unique, URL-safe identifier for this provider (e.g. "github", "google"). Embedded in the
    /// initiate route as <c>{basePath}/oauth/{Key}</c>.
    /// </summary>
    public string Key { get; set; } = string.Empty;

    public string? DisplayName { get; set; }

    public string AuthorizationUrl { get; set; } = string.Empty;
    public string TokenUrl { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;

    /// <summary>
    /// Null = public/PKCE-only client. Non-null = confidential client; included in the
    /// server-side token-endpoint POST (never sent to or visible from the browser).
    /// </summary>
    public string? ClientSecret { get; set; }

    public IList<string> Scopes { get; set; } = new List<string>();

    /// <summary>
    /// Used to fetch identity when the token response has no <c>id_token</c> (e.g. GitHub's
    /// classic OAuth, which is not OIDC-compliant).
    /// </summary>
    public string? UserInfoUrl { get; set; }

    public string? Icon { get; set; }
    public string? BackgroundColor { get; set; }
    public string? ForegroundColor { get; set; }
}
