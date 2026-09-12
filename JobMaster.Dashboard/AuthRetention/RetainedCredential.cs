namespace JobMaster.Dashboard.AuthRetention;

public sealed class RetainedCredential
{
    public IReadOnlyDictionary<string, string> Secrets { get; set; } = new Dictionary<string, string>();
    public DateTime ExpiresAt { get; set; }
}
