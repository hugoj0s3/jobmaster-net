namespace JobMaster.Dashboard.Endpoints;

internal sealed class CredentialsRequest
{
    public Dictionary<string, string> Secrets { get; set; } = new Dictionary<string, string>();
    public TimeSpan? DurationToExpire { get; set; }
}
