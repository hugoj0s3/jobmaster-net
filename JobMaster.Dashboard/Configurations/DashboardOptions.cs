using System.Text;
using JobMaster.Dashboard.Configurations.Auth;
using JobMaster.Dashboard.Configurations.Themes;

namespace JobMaster.Dashboard.Configurations;

internal class DashboardOptions
{
    /// <summary>
    /// The base path for the dashboard. Defaults to "/jm-dashboard".
    /// </summary>
    public string BasePath { get; set; } = "/jm-dashboard";

    /// <summary>
    /// Base URL of the JobMaster API the dashboard will connect to.
    /// </summary>
    public string ApiUrl { get; set; } = string.Empty;

    /// <summary>
    /// Configuration for the clusters that the dashboard can connect to.
    /// </summary>
    public IList<DashboardClusterConfig> Clusters { get; set; } = new List<DashboardClusterConfig>();

    public DashboardThemesConfig ThemeConfigs { get; set; } = new DashboardThemesConfig();

    public DashboardAuthConfig Auth { get; set; } = new DashboardAuthConfig();

    public DashboardAuthRetentionConfig AuthRetention { get; set; } = new DashboardAuthRetentionConfig();

    /// <summary>
    /// Controls OpenAPI-based auto-configuration.
    /// null = disabled. Empty string = auto-discover from content root.
    /// Any other value = explicit file path or absolute URL.
    /// </summary>
    public string? OpenApiUrl { get; set; }

    /// <summary>
    /// HttpOnly session cookie name, derived from <see cref="BasePath"/>.
    /// e.g. "/jm-dashboard" → "jm-dashboard-credentials-session"
    /// </summary>
    public string SessionCookieName => $"{DeriveSlug(BasePath)}-credentials-session";

    private static string DeriveSlug(string basePath)
    {
        var path = basePath.TrimStart('/');
        var sb = new StringBuilder(path.Length);
        var lastWasDash = false;
        foreach (var c in path)
        {
            if (char.IsLetterOrDigit(c))
            {
                sb.Append(char.ToLowerInvariant(c));
                lastWasDash = false;
            }
            else if (!lastWasDash)
            {
                sb.Append('-');
                lastWasDash = true;
            }
        }
        return sb.ToString().Trim('-');
    }
}
