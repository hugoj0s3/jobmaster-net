namespace JobMaster.Dashboard.Configurations.Public;

public class DashboardPublicConfig
{
    public string ApiBaseUrl { get; set; } = string.Empty;
    public string BasePath { get; set; } = string.Empty;
    /// <summary>"none" | "client" | "server"</summary>
    public string AuthRetentionMode { get; set; } = "none";
    public PublicAuthConfig Auth { get; set; } = new();
    public IList<DashboardPublicClusterConfig> Clusters { get; set; } = new List<DashboardPublicClusterConfig>();
    public PublicThemeConfig ThemeConfigs { get; set; } = new();
}
