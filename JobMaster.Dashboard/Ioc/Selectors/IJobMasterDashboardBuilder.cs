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
    /// Adds a cluster to the dashboard.
    /// </summary>
    IJobMasterDashboardBuilder AddCluster(string id, string environmentName);

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
    /// </summary>
    Auth.IJobMasterDashboardApiKeyAuthSelector AddApiKeyAuth();

    /// <summary>
    /// Configures username/password authentication.
    /// </summary>
    Auth.IJobMasterDashboardUserPasswordAuthSelector AddUserPasswordAuth();

    /// <summary>
    /// Configures simple JWT authentication where the user pastes a token directly.
    /// </summary>
    Auth.IJobMasterDashboardSimpleJwtAuthSelector AddSimpleJwtAuth();

    /// <summary>
    /// Configures JWT authentication with a custom login form that posts to a token endpoint.
    /// </summary>
    Auth.IJobMasterDashboardJwtFormAuthSelector AddJwtFormAuth(string tokenUrl);

    /// <summary>
    /// Auto-discovers an OpenAPI spec file under the app's content root.
    /// Looks for openapi.json, swagger/v1/swagger.json, swagger.json — throws at startup if none is found.
    /// </summary>
    IJobMasterDashboardBuilder FromOpenApi();

    /// <summary>
    /// Configures auth providers and clusters from an OpenAPI spec.
    /// Provide a relative or absolute file path, or an absolute URL (https://...).
    /// </summary>
    IJobMasterDashboardBuilder FromOpenApi(string urlOrPath);

    /// <summary>
    /// Configures session persistence for authentication.
    /// </summary>
    Credentials.IJobMasterDashboardCredentialsPersistenceSelector ConfigureCredentialsPersistence();
}