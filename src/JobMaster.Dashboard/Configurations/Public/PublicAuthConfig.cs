namespace JobMaster.Dashboard.Configurations.Public;

public class PublicAuthConfig
{
    public bool Enabled { get; set; }
    public IList<PublicAuthProviderConfig> Providers { get; set; } = new List<PublicAuthProviderConfig>();

    /// <summary>
    /// Label for the merged OAuth tab (every configured OAuth provider is grouped under one tab).
    /// Null unless explicitly set via <c>ConfigOAuth().WithTabLabel(...)</c> — the frontend falls
    /// back to "OAuth" itself when this is null.
    /// </summary>
    public string? OAuthTabLabel { get; set; }

    /// <summary>
    /// Short checkbox label gating every OAuth provider button. Null unless explicitly set via
    /// <c>ConfigOAuth().WithConsentText(...)</c> — the frontend shows no gate at all when this is null.
    /// </summary>
    public string? OAuthConsentCheckboxLabel { get; set; }

    /// <summary>
    /// Optional longer text shown via a "Learn more" disclosure next to <see cref="OAuthConsentCheckboxLabel"/>.
    /// </summary>
    public string? OAuthConsentDetailsText { get; set; }
}

