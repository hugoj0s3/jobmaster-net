namespace JobMaster.Dashboard.Configurations;

/// <summary>
/// Configures how dashboard auth credentials are retained across page refreshes.
/// Controls the storage mechanism, cookie identity, and expiry behaviour for user sessions.
/// </summary>
internal class DashboardAuthRetentionConfig
{
    /// <summary>
    /// Determines where and how credentials are stored. Defaults to <see cref="DashboardAuthRetentionType.NoRetention"/>.
    /// </summary>
    public DashboardAuthRetentionType AuthRetentionType { get; set; } = DashboardAuthRetentionType.NoRetention;

    /// <summary>
    /// How long a stored credential remains valid. Used as the fallback when no per-credential expiry is set.
    /// Defaults to 30 minutes.
    /// </summary>
    public TimeSpan DefaultCredentialsExpiry { get; set; } = TimeSpan.FromMinutes(30);

    internal static readonly TimeSpan SessionIdleExpiry = TimeSpan.FromHours(24);
}
