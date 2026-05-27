namespace JobMaster.Dashboard.Configurations.Public;

public class DashboardPublicConfig
{
    public string ApiBaseUrl { get; set; } = string.Empty;
    public string BasePath { get; set; } = string.Empty;
    public bool CredentialsPersistenceEnabled { get; set; }
    public PublicAuthConfig Auth { get; set; } = new();
    public IList<DashboardPublicClusterConfig> Clusters { get; set; } = new List<DashboardPublicClusterConfig>();
    public PublicThemeConfig ThemeConfigs { get; set; } = new();
}
