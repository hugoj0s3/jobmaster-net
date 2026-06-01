using JobMaster.Dashboard.Configurations.Auth;

namespace JobMaster.Dashboard.Ioc.Selectors;

public interface IJobMasterDashboardBuilder
{
    /// <summary>
    /// Sets the base path where the dashboard will be hosted.
    /// </summary>
    IJobMasterDashboardBuilder UseBasePath(string basePath);

    /// <summary>
    /// Sets the base URL of the JobMaster API the dashboard will connect to.
    /// </summary>
    IJobMasterDashboardBuilder UseApiUrl(string apiUrl);

    /// <summary>
    /// Configures a cluster entry for the dashboard.
    /// If a cluster with the same <paramref name="id"/> was already added, it is updated in place.
    /// </summary>
    /// <param name="id">The cluster identifier as reported by the JobMaster API.</param>
    /// <param name="environmentName">Display name shown in the dashboard. Defaults to <paramref name="id"/> when not provided.</param>
    /// <param name="disabled">
    /// When <see langword="true"/> the cluster is hidden from the dashboard even if the API reports it.
    /// </param>
    IJobMasterDashboardBuilder ConfigCluster(string id, string? environmentName = null, bool disabled = false);

    /// <summary>
    /// Prevents a cluster from appearing in the dashboard even when the API reports it.
    /// Equivalent to <c>ConfigCluster(id, disabled: true)</c>.
    /// Takes precedence over any auto-discovered cluster with the same <paramref name="id"/>.
    /// </summary>
    IJobMasterDashboardBuilder DisableCluster(string id);

    /// <summary>
    /// Adds a theme with a generated ID from the display name.
    /// </summary>
    Themes.IJobMasterDashboardThemeSelector AddTheme(Configurations.Themes.DashboardBuiltInTheme theme, string? displayName = null);

    /// <summary>
    /// Adds a theme and sets it as the primary theme.
    /// </summary>
    Themes.IJobMasterDashboardPrimaryThemeSelector AddPrimaryTheme(Configurations.Themes.DashboardBuiltInTheme theme, string? displayName = null);

    /// <summary>
    /// Configures API key authentication.
    /// If this auth type was already configured, the existing instance is returned for further customisation.
    /// </summary>
    Auth.IJobMasterDashboardApiKeyAuthSelector ConfigApiKeyAuth();

    /// <summary>
    /// Configures username/password authentication.
    /// If this auth type was already configured, the existing instance is returned for further customisation.
    /// </summary>
    Auth.IJobMasterDashboardUserPasswordAuthSelector ConfigUserPasswordAuth();

    /// <summary>
    /// Configures simple JWT authentication where the user pastes a token directly.
    /// If this auth type was already configured, the existing instance is returned for further customisation.
    /// </summary>
    Auth.IJobMasterDashboardSimpleJwtAuthSelector ConfigSimpleJwtAuth();

    /// <summary>
    /// Configures JWT authentication with a custom login form that posts to a token endpoint.
    /// If this auth type was already configured, the existing instance is returned for further customisation.
    /// </summary>
    Auth.IJobMasterDashboardJwtFormAuthSelector ConfigJwtFormAuth(string tokenUrl);

    /// <summary>
    /// Disables the specified auth type, preventing it from appearing in the dashboard
    /// even if the API reports it. Takes precedence over any auto-discovered auth provider
    /// of the same type.
    /// </summary>
    IJobMasterDashboardBuilder DisableAuth(DashboardAuthProviderId providerId);

    /// <summary>
    /// Configures auth providers and clusters from a JobMaster OpenAPI JSON spec.
    /// Provide a relative or absolute file path ending in <c>.json</c>, or an absolute URL (<c>https://...</c>).
    /// Leave empty or omit the path to auto-discover from the configured API URL.
    /// </summary>
    IJobMasterDashboardBuilder FromOpenApiJson(string urlOrPath = "");

    /// <summary>
    /// Configures auth retention for authentication.
    /// </summary>
    AuthRetention.IJobMasterDashboardAuthRetentionSelector ConfigureAuthRetention();
}
