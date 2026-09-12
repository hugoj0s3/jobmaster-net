namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class UserPasswordAuthProviderConfig : DashboardAuthTypeConfig
{
    public override DashboardAuthType AuthType => DashboardAuthType.UserPassword;
    public string UserHeaderName { get; set; } = "X-User-Name";
    public string PasswordHeaderName { get; set; } = "X-Password";
}
