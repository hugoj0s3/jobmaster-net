namespace JobMaster.Dashboard.Configurations;

/// <summary>
/// Configures how dashboard credentials are persisted across page refreshes.
/// Controls the storage mechanism, cookie identity, and expiry behaviour for user sessions.
/// </summary>
internal class DashboardCredentialsPersistenceConfig
{
    /// <summary>
    /// Determines where and how credentials are stored. Defaults to <see cref="DashboardCredentialsPersistenceType.NoPersistence"/>.
    /// </summary>
    public DashboardCredentialsPersistenceType PersistenceType { get; set; } = DashboardCredentialsPersistenceType.NoPersistence;

    /// <summary>
    /// How long a stored credential remains valid. Used as the fallback when no per-credential expiry is set.
    /// Defaults to 30 minutes.
    /// </summary>
    public TimeSpan DefaultCredentialsExpiry { get; set; } = TimeSpan.FromMinutes(30);

    internal static readonly TimeSpan SessionIdleExpiry = TimeSpan.FromHours(24);
    internal const int OpenSessionRateLimit = 5;
    internal static readonly TimeSpan OpenSessionRateWindow = TimeSpan.FromSeconds(60);
}
