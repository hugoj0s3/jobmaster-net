namespace JobMaster.Dashboard.Configurations;

/// <summary>
/// Defines how the browser/server retains auth credentials on refresh.
/// </summary>
public enum DashboardAuthRetentionType
{
    /// <summary>No retention. Credentials are lost on page refresh.</summary>
    NoRetention = 1,

    /// <summary>Server-side memory cache with HttpOnly cookie. Single-server, lost on app restart.</summary>
    ServerSideInMemory = 2,

    /// <summary>Server-side distributed cache with HttpOnly cookie. Requires IDistributedCache. Multi-server safe.</summary>
    ServerSideDistributed = 3,

    /// <summary>
    /// Browser sessionStorage. Credentials survive refresh but are vulnerable to XSS.
    /// Use only for development or trusted internal environments.
    /// </summary>
    ClientSideSessionStorage = 4,
}
