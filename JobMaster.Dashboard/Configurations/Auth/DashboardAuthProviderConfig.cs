namespace JobMaster.Dashboard.Configurations.Auth;

internal abstract class DashboardAuthProviderConfig
{
    public abstract DashboardAuthProviderId ProviderId { get; }
    public string? DisplayName { get; set; }
    /// <summary>
    /// When <see langword="true"/> this auth type is hidden from the dashboard even if the API reports it.
    /// </summary>
    public bool Disabled { get; set; }
}

