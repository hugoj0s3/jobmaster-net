namespace JobMaster.Dashboard.Configurations.Public;

public class PublicAuthProviderConfig
{
    public string Type { get; set; } = string.Empty;
    public string? DisplayName { get; set; }

    // API_KEY + JWT_SIMPLE
    public string? HeaderName { get; set; }

    // JWT_SIMPLE
    public string? Scheme { get; set; }

    // USER_PASSWORD
    public string? UserHeaderName { get; set; }
    public string? PasswordHeaderName { get; set; }

    // JWT_CUSTOM_FORM
    public string? TokenUrl { get; set; }
    public PublicJwtTransportConfig? Transport { get; set; }
    public IList<PublicJwtFormFieldConfig>? Fields { get; set; }
}
