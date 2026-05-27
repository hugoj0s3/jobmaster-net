namespace JobMaster.Dashboard.Configurations.Public;

public class PublicJwtTransportConfig
{
    public string Header { get; set; } = "Authorization";
    public string Scheme { get; set; } = "Bearer";
}
