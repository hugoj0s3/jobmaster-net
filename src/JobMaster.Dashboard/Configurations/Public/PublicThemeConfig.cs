using JobMaster.Dashboard.Configurations.Themes;

namespace JobMaster.Dashboard.Configurations.Public;

public class PublicThemeConfig
{
    public string PrimaryThemeId { get; set; } = string.Empty;
    public IList<PublicThemeItemConfig> Themes { get; set; } = new List<PublicThemeItemConfig>();
}

