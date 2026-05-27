namespace JobMaster.Dashboard.Credentials;

internal interface IRateSessionLimiter
{
    Task<RateSessionResult> TryOpenSessionAsync(string clientId, string newSessionId, CancellationToken ct = default);
}
