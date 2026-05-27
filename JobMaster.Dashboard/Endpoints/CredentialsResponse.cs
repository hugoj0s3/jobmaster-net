namespace JobMaster.Dashboard.Endpoints;

internal sealed class CredentialsResponse
{
    public IReadOnlyDictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();
    public DateTime ExpiryAt { get; set; }
}
