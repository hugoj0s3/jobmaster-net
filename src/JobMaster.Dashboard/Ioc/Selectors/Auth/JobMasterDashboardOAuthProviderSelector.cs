using JobMaster.Dashboard.Configurations.Auth;

namespace JobMaster.Dashboard.Ioc.Selectors.Auth;

internal sealed class JobMasterDashboardOAuthProviderSelector : JobMasterDashboardOAuthSelector, IJobMasterDashboardOAuthProviderSelector
{
    private readonly OAuthProviderConfig current;

    public JobMasterDashboardOAuthProviderSelector(OAuthAuthConfig config, OAuthProviderConfig current)
        : base(config)
    {
        this.current = current;
    }

    public IJobMasterDashboardOAuthProviderSelector WithDisplayName(string displayName)
    {
        current.DisplayName = displayName;
        return this;
    }

    public IJobMasterDashboardOAuthProviderSelector WithClientSecret(string clientSecret)
    {
        current.ClientSecret = clientSecret;
        return this;
    }

    public IJobMasterDashboardOAuthProviderSelector WithScopes(params string[] scopes)
    {
        current.Scopes = scopes.ToList();
        return this;
    }

    public IJobMasterDashboardOAuthProviderSelector WithUserInfoUrl(string userInfoUrl)
    {
        current.UserInfoUrl = userInfoUrl;
        return this;
    }

    public IJobMasterDashboardOAuthProviderSelector WithIcon(string iconUrl)
    {
        current.Icon = iconUrl;
        return this;
    }

    public IJobMasterDashboardOAuthProviderSelector WithBackgroundColor(string color)
    {
        current.BackgroundColor = color;
        return this;
    }

    public IJobMasterDashboardOAuthProviderSelector WithForegroundColor(string color)
    {
        current.ForegroundColor = color;
        return this;
    }
}
