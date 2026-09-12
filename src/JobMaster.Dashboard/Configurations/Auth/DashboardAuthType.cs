namespace JobMaster.Dashboard.Configurations.Auth;

/// <summary>
/// Identifies which kind of authentication mechanism a <see cref="DashboardAuthTypeConfig"/>
/// configures. Exactly one config exists per value — for <see cref="OAuth"/>, that single config
/// (<see cref="OAuthAuthConfig"/>) holds a list of individual OAuth providers (Google, GitHub, etc.)
/// rather than one config per identity provider.
/// </summary>
public enum DashboardAuthType
{
    ApiKey = 1,
    UserPassword = 2,
    SimpleJwt = 3,
    JwtForm = 4,
    OAuth = 5
}
