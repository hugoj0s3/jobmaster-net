namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

/// <summary>
/// Per-provider OAuth configuration selector, returned by <see cref="IJobMasterDashboardOAuthSelector.AddOAuthProvider(string,string,string,string,string)"/>.
/// Extends <see cref="IJobMasterDashboardOAuthSelector"/>, so every group-level method
/// (<c>WithTokenIssuer</c>, <c>WithStorage</c>, another <c>AddOAuthProvider(...)</c>, ...) stays
/// directly callable from here too — there's no separate step needed to "finish" a provider and
/// go back to the group. Whichever one you call, it's the same shared OAuth config underneath.
/// </summary>
public interface IJobMasterDashboardOAuthProviderSelector
    : IJobMasterDashboardOAuthSelector, IJobMasterDashboardAuthProviderSelector<IJobMasterDashboardOAuthProviderSelector>
{
    /// <summary>
    /// Enables confidential-client mode: <c>client_secret</c> is included in the server-side
    /// token-endpoint POST. Never sent to or visible from the browser.
    /// </summary>
    IJobMasterDashboardOAuthProviderSelector WithClientSecret(string clientSecret);

    IJobMasterDashboardOAuthProviderSelector WithScopes(params string[] scopes);

    /// <summary>
    /// Used to fetch identity when the token response has no <c>id_token</c> (e.g. GitHub's
    /// classic OAuth, which is not OIDC-compliant).
    /// </summary>
    IJobMasterDashboardOAuthProviderSelector WithUserInfoUrl(string userInfoUrl);

    IJobMasterDashboardOAuthProviderSelector WithIcon(string iconUrl);
    IJobMasterDashboardOAuthProviderSelector WithBackgroundColor(string color);
    IJobMasterDashboardOAuthProviderSelector WithForegroundColor(string color);
}
