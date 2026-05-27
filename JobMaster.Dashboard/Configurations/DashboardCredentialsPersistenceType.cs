namespace JobMaster.Dashboard.Configurations;

/// <summary>
/// Define how the browser/server will deal with the credential resilence on refresh.
/// </summary>
public enum DashboardCredentialsPersistenceType
{
    /// <summary>No Persistence. Credentials are lost on page refresh.</summary>
    NoPersistence = 1,

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
