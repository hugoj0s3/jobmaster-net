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

    /// <summary>
    /// Set by <c>WithConsentText(...)</c>. Short label shown directly next to a checkbox under the
    /// OAuth provider buttons — every provider button stays disabled until it's checked. Once
    /// checked, the browser remembers it (keyed by a hash of this text plus <see cref="ConsentDetailsText"/>,
    /// so editing either invalidates every prior consent) and the gate never shows again on that
    /// browser. Null unless set, in which case no gate is shown at all.
    /// </summary>
    public string? ConsentCheckboxLabel { get; set; }

    /// <summary>
    /// Optional longer text revealed via a "Learn more" disclosure next to <see cref="ConsentCheckboxLabel"/>
    /// — for the common pattern of a short checkbox label ("I agree to how sign-in data is used")
    /// backed by a fuller explanation the visitor can expand if they want it. Has no effect unless
    /// <see cref="ConsentCheckboxLabel"/> is also set.
    /// </summary>
    public string? ConsentDetailsText { get; set; }
}
