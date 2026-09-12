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

    /// <summary>
    /// When set via <see cref="Ioc.Selectors.AuthRetention.IJobMasterDashboardAuthRetentionSelector.UseCustom{T}"/>,
    /// this type is registered as <see cref="AuthRetention.IJobMasterAuthRetentionStorage"/> instead of the
    /// built-in implementation matching <see cref="AuthRetentionType"/>.
    /// </summary>
    public Type? CustomAuthRetentionStorageType { get; set; }

    internal static readonly TimeSpan SessionIdleExpiry = TimeSpan.FromHours(24);
}
