namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class SimpleJwtAuthProviderConfig : DashboardAuthProviderConfig
{
    public override DashboardAuthProviderId ProviderId => DashboardAuthProviderId.SimpleJwt;
    public string HeaderName { get; set; } = "Authorization";
    public string Scheme { get; set; } = "Bearer";
}
