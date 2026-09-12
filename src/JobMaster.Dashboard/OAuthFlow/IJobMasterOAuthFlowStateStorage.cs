namespace JobMaster.Dashboard.OAuthFlow;

/// <summary>
/// Stores the transient state/PKCE-verifier of an in-flight OAuth login between the initiate
/// and confirm steps. Implement this to plug in a custom backend via
/// <see cref="Ioc.Selectors.Auth.IJobMasterDashboardOAuthSelector.UseCustomStorage{T}"/>.
/// </summary>
public interface IJobMasterOAuthFlowStateStorage
{
    /// <summary>
    /// Persists <paramref name="state"/> with a short TTL and returns the value that should be
    /// set as the flow cookie.
    /// </summary>
    Task<string> BeginAsync(OAuthFlowState state);

    /// <summary>
    /// Validates/decodes <paramref name="cookieValue"/> and, for server-backed implementations,
    /// deletes the record — true single-use. Returns null if missing, invalid, or expired.
    /// </summary>
    Task<OAuthFlowState?> ConsumeAsync(string cookieValue);
}
