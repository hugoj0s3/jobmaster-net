namespace JobMaster.Dashboard.Configurations.Auth;

internal sealed class JwtFormAuthProviderConfig : DashboardAuthTypeConfig
{
    public override DashboardAuthType AuthType => DashboardAuthType.JwtForm;
    public string TokenUrl { get; set; } = string.Empty;
    public string HeaderName { get; set; } = "Authorization";
    public string Scheme { get; set; } = "Bearer";
    public IList<JwtFormFieldConfig> Fields { get; init; } = new List<JwtFormFieldConfig>();
}
