namespace JobMaster.Dashboard.AuthRetention;

internal sealed class StoredAuth
{
    public IReadOnlyDictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();
    public DateTime ExpiresAt { get; set; }
}
