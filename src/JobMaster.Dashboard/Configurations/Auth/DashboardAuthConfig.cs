namespace JobMaster.Dashboard.Configurations.Auth;

internal class DashboardAuthConfig
{
    public bool Enabled { get; set; } = false;
    public IList<DashboardAuthTypeConfig> Providers { get; set; } = new List<DashboardAuthTypeConfig>();
}
