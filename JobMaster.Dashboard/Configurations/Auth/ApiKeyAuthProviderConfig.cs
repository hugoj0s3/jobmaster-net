namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class ApiKeyAuthProviderConfig : DashboardAuthProviderConfig
{
    public override DashboardAuthProviderId ProviderId => DashboardAuthProviderId.ApiKey;
    public string HeaderName { get; set; } = "X-Api-Key";
}
