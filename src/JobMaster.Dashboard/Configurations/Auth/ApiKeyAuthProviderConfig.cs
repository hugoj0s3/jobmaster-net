namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class ApiKeyAuthProviderConfig : DashboardAuthTypeConfig
{
    public override DashboardAuthType AuthType => DashboardAuthType.ApiKey;
    public string HeaderName { get; set; } = "X-Api-Key";
}
