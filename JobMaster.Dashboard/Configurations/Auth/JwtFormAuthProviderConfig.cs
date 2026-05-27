namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class JwtFormAuthProviderConfig : DashboardAuthProviderConfig
{
    public override DashboardAuthProviderId ProviderId => DashboardAuthProviderId.JwtForm;
    public string TokenUrl { get; set; } = string.Empty;
    public string HeaderName { get; set; } = "Authorization";
    public string Scheme { get; set; } = "Bearer";
    public IList<JwtFormFieldConfig> Fields { get; init; } = new List<JwtFormFieldConfig>();
}
