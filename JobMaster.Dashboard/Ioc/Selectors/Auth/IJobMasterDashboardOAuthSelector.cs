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
}
