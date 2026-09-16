namespace JobMaster.Dashboard.OAuthFlow;

/// <summary>
/// The transient state of an in-flight OAuth login, held between the initiate and confirm steps.
/// </summary>
public sealed class OAuthFlowState
{
    public string ProviderKey { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
    public string CodeVerifier { get; set; } = string.Empty;

    /// <summary>
    /// Whether the consent checkbox (see <c>WithConsentText</c>) was checked when this login was
    /// initiated — carried across the redirect round-trip to the IdP and back so it's still
    /// available at the confirm step, where it's copied onto
    /// <c>OAuthUserIdentity.Claims[OAuthUserIdentity.ConsentClaimKey]</c>. Always <c>false</c> when
    /// no consent gate is configured.
    /// </summary>
    public bool Consent { get; set; }
}
