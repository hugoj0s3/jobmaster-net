namespace JobMaster.Dashboard.Configurations.Auth;

internal abstract class DashboardAuthProviderConfig
{
    public abstract DashboardAuthProviderId ProviderId { get; }
    public string? DisplayName { get; set; }
}

