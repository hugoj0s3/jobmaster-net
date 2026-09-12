using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.OAuthFlow;

namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

internal class JobMasterDashboardOAuthSelector : IJobMasterDashboardOAuthSelector
{
    protected readonly OAuthAuthConfig config;

    public JobMasterDashboardOAuthSelector(OAuthAuthConfig config)
    {
        this.config = config;
    }

    public IJobMasterDashboardOAuthProviderSelector AddOAuthProvider(
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
        string? foregroundColor = null)
    {
        var existing = config.Providers.FirstOrDefault(p => p.Key == key);
        var provider = existing ?? new OAuthProviderConfig { Key = key };

        provider.DisplayName = displayName;
        provider.AuthorizationUrl = authorizationUrl;
        provider.TokenUrl = tokenUrl;
        provider.ClientId = clientId;
        if (clientSecret is not null) provider.ClientSecret = clientSecret;
        if (scopes is not null) provider.Scopes = scopes.ToList();
        if (userInfoUrl is not null) provider.UserInfoUrl = userInfoUrl;
        if (icon is not null) provider.Icon = icon;
        if (backgroundColor is not null) provider.BackgroundColor = backgroundColor;
        if (foregroundColor is not null) provider.ForegroundColor = foregroundColor;

        if (existing is null) config.Providers.Add(provider);
        return new JobMasterDashboardOAuthProviderSelector(config, provider);
    }

    public IJobMasterDashboardOAuthSelector WithTokenIssuer(Func<OAuthUserIdentity, Task<string>> issuer)
    {
        config.TokenIssuer = issuer;
        return this;
    }

    public IJobMasterDashboardOAuthSelector OnLoginSucceeded(Func<OAuthUserIdentity, Task> callback)
    {
        config.OnLoginSucceeded = callback;
        return this;
    }

    public IJobMasterDashboardOAuthSelector WithStorage(OAuthFlowStateStorage storage)
    {
        config.FlowStateStorage = storage;
        return this;
    }

    public IJobMasterDashboardOAuthSelector UseCustomStorage<T>() where T : class, IJobMasterOAuthFlowStateStorage
    {
        config.FlowStateStorage = OAuthFlowStateStorage.Custom;
        config.CustomFlowStateStorageType = typeof(T);
        return this;
    }
}
