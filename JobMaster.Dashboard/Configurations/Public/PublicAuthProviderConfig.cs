namespace JobMaster.Dashboard.Configurations.Public;

public class PublicAuthProviderConfig
{
    public string Type { get; set; } = string.Empty;
    public string? DisplayName { get; set; }

    // API_KEY + JWT_SIMPLE + JWT_CUSTOM_FORM + OAUTH
    public string? HeaderName { get; set; }

    // JWT_SIMPLE + OAUTH
    public string? Scheme { get; set; }

    // USER_PASSWORD
    public string? UserHeaderName { get; set; }
    public string? PasswordHeaderName { get; set; }

    // JWT_CUSTOM_FORM + JWT_SIMPLE
    public string? TokenUrl { get; set; }
    public IList<PublicJwtFormFieldConfig>? Fields { get; set; }

    // OAUTH — disambiguates multiple OAUTH entries; used to hit GET {basePath}/oauth/{Key}
    public string? Key { get; set; }
    public string? Icon { get; set; }
    public string? BackgroundColor { get; set; }
    public string? ForegroundColor { get; set; }
}
