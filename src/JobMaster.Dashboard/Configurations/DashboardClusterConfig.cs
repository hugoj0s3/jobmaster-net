namespace JobMaster.Dashboard.Configurations;

public class DashboardClusterConfig
{
    public string? Id { get; set; }
    public string? EnvironmentName { get; set; }
    public string? ThemeId { get; set; }
    /// <summary>
    /// When <see langword="true"/> this cluster is hidden from the dashboard even if the API reports it.
    /// </summary>
    public bool Disabled { get; set; }
}
