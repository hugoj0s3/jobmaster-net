using Microsoft.AspNetCore.Http;

namespace JobMaster.Dashboard.Configurations.Auth;

/// <summary>
/// The identity established from an OAuth provider after a successful code exchange,
/// handed to the configured token-issuer hook to mint a JobMaster-aligned JWT, and to the
/// post-login hook for side effects (analytics, lead tracking, etc.).
/// </summary>
public sealed class OAuthUserIdentity
{
    /// <summary>
    /// Key under <see cref="Claims"/> for the framework-injected consent flag — see the property's
    /// own doc for why this is namespaced rather than a plain "consent" key. Exposed as a constant
    /// for the rare case a consumer needs the raw claim; prefer <see cref="IsConsent"/> otherwise.
    /// </summary>
    public const string ConsentClaimKey = "jobmaster_consent";

    /// <summary>
    /// Whether the consent checkbox (see <c>ConfigOAuth().WithConsentText</c>) was checked at
    /// login. <c>false</c> when no consent gate is configured. A <c>WithTokenIssuer</c>/
    /// <c>OnLoginSucceeded</c> hook that records leads or other consent-gated side effects should
    /// check this before doing so, e.g. <c>if (!identity.IsConsent) return;</c>.
    /// </summary>
    public bool IsConsent() => Claims.TryGetValue(ConsentClaimKey, out var consent) && consent == "1";

    /// <summary>
    /// Stable subject identifier, provider-prefixed (e.g. "github:hugoj0s3") so two different
    /// providers can never collide on the same subject string. Equivalent to
    /// <c>$"{ProviderKey}:{ProviderUserId}"</c> — kept as a single string since that's the shape
    /// a JWT's own <c>sub</c> claim needs.
    /// </summary>
    public string Subject { get; set; } = string.Empty;

    /// <summary>
    /// The key of the OAuth provider that authenticated this user (e.g. "github", "google") —
    /// matches whatever key was passed to <c>AddOAuthProvider(key, ...)</c>.
    /// </summary>
    public string ProviderKey { get; set; } = string.Empty;

    /// <summary>
    /// The user's id as assigned by the provider itself (e.g. GitHub's numeric user id, or a
    /// Google account's <c>sub</c> claim) — unique within that provider, but not necessarily
    /// across providers (that's what pairing it with <see cref="ProviderKey"/> is for).
    /// </summary>
    public string ProviderUserId { get; set; } = string.Empty;

    /// <summary>
    /// Claims from the IdP's token/userinfo response, plus two framework-injected keys: "provider"
    /// (same value as <see cref="ProviderKey"/>) and <see cref="ConsentClaimKey"/> ("1"/"0",
    /// whether the consent checkbox — see <c>ConfigOAuth().WithConsentText</c> — was checked at
    /// login; always "0" when no consent gate is configured). The latter is deliberately
    /// namespaced rather than a plain "consent" key, since this dictionary is seeded from the
    /// IdP's own response first — an unprefixed key could collide with and silently overwrite a
    /// real claim the provider happens to return under that name.
    /// </summary>
    public IDictionary<string, string> Claims { get; set; } = new Dictionary<string, string>();

    /// <summary>
    /// The request that completed this login. Useful for anything request-scoped a token-issuer
    /// or post-login hook might need (e.g. the caller's IP address for a login-activity log) —
    /// null only if an <see cref="OAuthUserIdentity"/> is constructed outside the normal OAuth
    /// confirm flow (e.g. in a unit test).
    /// </summary>
    public HttpContext? HttpContext { get; set; }
}
