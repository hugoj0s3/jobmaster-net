namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class SimpleJwtAuthProviderConfig : DashboardAuthTypeConfig
{
    public override DashboardAuthType AuthType => DashboardAuthType.SimpleJwt;
    public string HeaderName { get; set; } = "Authorization";
    public string Scheme { get; set; } = "Bearer";
}
