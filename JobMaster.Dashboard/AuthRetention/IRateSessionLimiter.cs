namespace JobMaster.Dashboard.AuthRetention;

internal interface IRateSessionLimiter
{
    Task<RateSessionResult> TryOpenSessionAsync(string clientId, string newSessionId, CancellationToken ct = default);
}
