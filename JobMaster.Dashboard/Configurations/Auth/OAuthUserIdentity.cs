namespace JobMaster.Dashboard.Configurations.Auth;

/// <summary>
/// The identity established from an OAuth provider after a successful code exchange,
/// handed to the configured token-issuer hook to mint a JobMaster-aligned JWT.
/// </summary>
public sealed class OAuthUserIdentity
{
    /// <summary>
    /// Stable subject identifier, provider-prefixed (e.g. "github:hugoj0s3") so two different
    /// providers can never collide on the same subject string.
    /// </summary>
    public string Subject { get; set; } = string.Empty;

    public IDictionary<string, string> Claims { get; set; } = new Dictionary<string, string>();
}
