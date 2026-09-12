namespace JobMaster.Dashboard.OAuthFlow;

/// <summary>
/// The transient state of an in-flight OAuth login, held between the initiate and confirm steps.
/// </summary>
public sealed class OAuthFlowState
{
    public string ProviderKey { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
    public string CodeVerifier { get; set; } = string.Empty;
}
