using JobMaster.Dashboard.Configurations.Themes;

namespace JobMaster.Dashboard.Configurations.Public;

public class PublicThemeItemConfig
{
    public string Id { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public string BaseTheme { get; set; } = string.Empty;
    public bool IsPrimaryTheme { get; set; }
    public DashboardPublicColorOverrides? ColorOverrides { get; set; }
    public DashboardStyleAttributeOverrides? StyleOverrides { get; set; }
}
