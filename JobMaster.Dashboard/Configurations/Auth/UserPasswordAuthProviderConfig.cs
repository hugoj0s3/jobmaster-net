namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class UserPasswordAuthProviderConfig : DashboardAuthProviderConfig
{
    public override DashboardAuthProviderId ProviderId => DashboardAuthProviderId.UserPassword;
    public string UserHeaderName { get; set; } = "X-User-Name";
    public string PasswordHeaderName { get; set; } = "X-Password";
}
