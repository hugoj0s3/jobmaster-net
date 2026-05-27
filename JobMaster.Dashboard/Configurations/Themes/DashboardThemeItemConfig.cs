namespace JobMaster.Dashboard.Configurations.Themes;

internal class DashboardThemeItemConfig
{
    /// <summary>
    /// Defines the base theme of the dashboard configuration.
    /// The base theme determines the overall visual style by selecting a predefined
    /// theme variation from the set of available themes.
    /// </summary>
    public DashboardBuiltInTheme BaseTheme { get; set; } = DashboardBuiltInTheme.JobMasterLight;

    /// <summary>
    /// Represents the display name of the theme configuration.
    /// The display name is a human-readable label that provides a clear description
    /// or title of the current theme, aiding in its identification and selection
    /// within the dashboard settings.
    /// </summary>
    public string DisplayName { get; set; } = null!;

    /// <summary>
    /// Specifies the customizable color overrides for the theme configuration.
    /// Overrides allow modifying specific color values for the dashboard theme,
    /// such as primary, secondary, and accent colors, providing a way to fine-tune the visual appearance.
    /// </summary>
    public DashboardPublicColorOverrides ColorOverrides { get; set; } = new();

    /// <summary>
    /// Defines the overrides for style attributes within the customized theme configuration.
    /// These overrides allow fine-tuning of various visual aspects such as font, border radii,
    /// button animations, and more, ensuring a tailored look and feel.
    /// It only possible for the primary theme.
    /// </summary>
    public DashboardStyleAttributeOverrides StyleOverrides { get; set; } = new();

    /// <summary>
    /// Indicates whether this theme is the primary theme used within the application.
    /// A primary theme generally serves as the main visual representation and branding style
    /// for the user interface, and typically influences the default appearance settings
    /// across the application.
    /// Only one Primary theme is allowed
    /// Only primary overrides "styles attributes"
    /// The others themes will follow the primary style attribute.
    /// </summary>
    public bool IsPrimaryTheme { get; set; } = false;
}
