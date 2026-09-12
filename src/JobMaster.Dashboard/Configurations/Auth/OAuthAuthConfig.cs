namespace JobMaster.Dashboard.Configurations.Auth;

/// <summary>
/// The single OAuth auth config — like every other <see cref="DashboardAuthTypeConfig"/>,
/// exactly one of these exists. Unlike the others, it holds a list of individual OAuth providers
/// (Google, GitHub, etc.) rather than configuring one provider directly, since OAuth is the one
/// auth type where multiple simultaneous providers are a real need.
/// </summary>
internal sealed class OAuthAuthConfig : DashboardAuthTypeConfig
{
    public override DashboardAuthType AuthType => DashboardAuthType.OAuth;

    public IList<OAuthProviderConfig> Providers { get; set; } = new List<OAuthProviderConfig>();

    /// <summary>
    /// Mints a JobMaster-aligned JWT from the identity established via any configured OAuth
    /// provider. Required if any OAuth provider is configured. Shared by every provider —
    /// regardless of which one authenticated the user, the resulting JWT has to align with the
    /// same JobMaster.Api, so there's no per-provider override.
    /// </summary>
    public Func<OAuthUserIdentity, Task<string>>? TokenIssuer { get; set; }

    /// <summary>
    /// Optional, best-effort side-effect hook fired after a successful login (analytics, lead
    /// tracking, etc.). Any exception it throws is swallowed — it never fails the login.
    /// </summary>
    public Func<OAuthUserIdentity, Task>? OnLoginSucceeded { get; set; }

    public OAuthFlowStateStorage FlowStateStorage { get; set; } = OAuthFlowStateStorage.Cookie;

    /// <summary>
    /// Set by <c>UseCustomStorage&lt;T&gt;()</c> alongside <see cref="FlowStateStorage"/> = <see cref="OAuthFlowStateStorage.Custom"/>.
    /// </summary>
    public Type? CustomFlowStateStorageType { get; set; }
}
