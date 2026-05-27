namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class UserPasswordAuthProviderConfig : DashboardAuthProviderConfig
{
    public override DashboardAuthProviderId ProviderId => DashboardAuthProviderId.UserPassword;
    public string UserHeaderName { get; set; } = "X-JobMaster-User";
    public string PasswordHeaderName { get; set; } = "X-JobMaster-Pwd";
}
