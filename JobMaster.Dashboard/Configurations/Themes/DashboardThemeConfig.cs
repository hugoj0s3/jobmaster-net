namespace JobMaster.Dashboard.Configurations.Themes;

internal class DashboardThemesConfig
{
    public string? PrimaryThemeId { get; set; }
    public IList<DashboardThemeItemConfig> Themes { get; set; } = new List<DashboardThemeItemConfig>();
}
