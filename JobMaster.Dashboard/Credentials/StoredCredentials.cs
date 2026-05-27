namespace JobMaster.Dashboard.Credentials;

internal sealed class StoredCredentials
{
    public IReadOnlyDictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();
    public DateTime ExpiresAt { get; set; }
}