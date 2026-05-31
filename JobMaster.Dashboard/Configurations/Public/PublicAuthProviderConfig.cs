namespace JobMaster.Dashboard.Configurations.Public;

public class PublicAuthProviderConfig
{
    public string Type { get; set; } = string.Empty;
    public string? DisplayName { get; set; }

    // API_KEY + JWT_SIMPLE + JWT_CUSTOM_FORM
    public string? HeaderName { get; set; }

    // JWT_SIMPLE
    public string? Scheme { get; set; }

    // USER_PASSWORD
    public string? UserHeaderName { get; set; }
    public string? PasswordHeaderName { get; set; }

    // JWT_CUSTOM_FORM + JWT_SIMPLE
    public string? TokenUrl { get; set; }
    public IList<PublicJwtFormFieldConfig>? Fields { get; set; }
}
