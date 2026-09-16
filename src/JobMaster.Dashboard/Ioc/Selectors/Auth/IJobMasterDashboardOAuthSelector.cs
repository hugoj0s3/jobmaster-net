using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.OAuthFlow;

namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

/// <summary>
/// Group-level OAuth configuration selector. Unlike every other <c>Config*Auth()</c> method (each
/// a singleton), OAuth supports multiple simultaneous providers — call <see cref="AddOAuthProvider(string,string,string,string,string)"/>
/// repeatedly to add as many as needed. Everything here — providers, the shared token-issuer
/// default, and flow-state storage — is consolidated under this one entry point. These methods
/// also stay reachable after configuring a specific provider, since <see cref="IJobMasterDashboardOAuthProviderSelector"/>
/// extends this interface — there's no need to "close" a provider before setting group-level options.
/// </summary>
public interface IJobMasterDashboardOAuthSelector
{
    /// <summary>
    /// Adds (or, if <paramref name="key"/> already exists, updates in place) an OAuth provider entry.
    /// The optional parameters can also be set afterward via the returned selector's <c>With...()</c>
    /// methods — both are equivalent, use whichever reads better at the call site.
    /// </summary>
    IJobMasterDashboardOAuthProviderSelector AddOAuthProvider(
        string key,
        string displayName,
        string authorizationUrl,
        string tokenUrl,
        string clientId,
        string? clientSecret = null,
        string[]? scopes = null,
        string? userInfoUrl = null,
        string? icon = null,
        string? backgroundColor = null,
        string? foregroundColor = null);

    /// <summary>
    /// Mints a JobMaster-aligned JWT from the identity established via any configured OAuth
    /// provider. Required if any OAuth provider is configured.
    /// </summary>
    IJobMasterDashboardOAuthSelector WithTokenIssuer(Func<OAuthUserIdentity, Task<string>> issuer);

    /// <summary>
    /// Optional, best-effort side-effect hook fired after a successful login. Any exception it
    /// throws is swallowed — it never fails the login.
    /// </summary>
    IJobMasterDashboardOAuthSelector OnLoginSucceeded(Func<OAuthUserIdentity, Task> callback);

    /// <summary>
    /// Selects which built-in flow-state storage backend is used. Defaults to <see cref="OAuthFlowStateStorage.Cookie"/>.
    /// </summary>
    IJobMasterDashboardOAuthSelector WithStorage(OAuthFlowStateStorage storage);

    /// <summary>
    /// Registers a custom <see cref="IJobMasterOAuthFlowStateStorage"/> implementation, taking
    /// precedence over <see cref="WithStorage"/>.
    /// </summary>
    IJobMasterDashboardOAuthSelector UseCustomStorage<T>() where T : class, IJobMasterOAuthFlowStateStorage;

    /// <summary>
    /// Sets the label shown for the merged OAuth tab in the dashboard's login screen (every
    /// configured provider is grouped under one tab, rather than one tab per provider). Defaults
    /// to "OAuth" if never called. Named distinctly from <c>WithDisplayName</c> (used per-provider,
    /// on the selector <see cref="AddOAuthProvider(string,string,string,string,string)"/> returns)
    /// to avoid the two colliding on a single selector.
    /// </summary>
    IJobMasterDashboardOAuthSelector WithTabLabel(string label);

    /// <summary>
    /// Gates every OAuth provider button under this <c>ConfigOAuth()</c> call behind a checkbox —
    /// buttons stay disabled until <paramref name="checkboxLabel"/> is checked. Once checked, the
    /// browser remembers it (via <c>localStorage</c>, keyed by a hash of the text so editing either
    /// parameter invalidates every prior consent) and the gate never shows again on that browser.
    /// Global to the whole OAuth group, not per-provider — every provider added under one
    /// <c>ConfigOAuth()</c> call shares the same login behavior and disclosure, so a per-provider
    /// split would just be two checkboxes describing the same thing.
    /// </summary>
    /// <param name="checkboxLabel">
    /// Short text shown directly next to the checkbox, e.g. "I agree to how sign-in data is used".
    /// Wrap one word/phrase in a single pair of square brackets, e.g. "I agree with [terms].", to
    /// turn it into the inline trigger for <paramref name="detailsText"/> instead of showing a
    /// separate "View details" link below the checkbox. Only the first bracket pair is honored;
    /// ignored if <paramref name="detailsText"/> is null.
    /// </param>
    /// <param name="detailsText">Optional longer explanation revealed via a "View details" disclosure next to (or inline within) the checkbox label.</param>
    IJobMasterDashboardOAuthSelector WithConsentText(string checkboxLabel, string? detailsText = null);
}
