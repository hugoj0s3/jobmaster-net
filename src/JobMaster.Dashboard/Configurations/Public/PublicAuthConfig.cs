namespace JobMaster.Dashboard.Configurations.Public;

public class PublicAuthConfig
{
    public bool Enabled { get; set; }
    public IList<PublicAuthProviderConfig> Providers { get; set; } = new List<PublicAuthProviderConfig>();
}

