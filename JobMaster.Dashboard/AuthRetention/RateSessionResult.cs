namespace JobMaster.Dashboard.AuthRetention;

internal sealed class RateSessionResult
{
    public bool Allowed { get; }
    public string? PreviousSessionId { get; }

    private RateSessionResult(bool allowed, string? previousSessionId)
    {
        Allowed = allowed;
        PreviousSessionId = previousSessionId;
    }

    public static RateSessionResult Denied { get; } = new RateSessionResult(false, null);
    public static RateSessionResult Granted(string? previousSessionId) => new RateSessionResult(true, previousSessionId);
}
