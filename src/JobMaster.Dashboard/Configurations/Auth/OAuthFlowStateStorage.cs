namespace JobMaster.Dashboard.Configurations.Auth;

/// <summary>
/// Where the transient state/PKCE-verifier for an in-flight OAuth login is held between
/// the initiate and confirm steps. Independent of <see cref="DashboardAuthRetentionType"/>,
/// which governs the long-lived post-login credential instead.
/// </summary>
public enum OAuthFlowStateStorage
{
    /// <summary>
    /// Self-contained, sealed via ASP.NET Core Data Protection, held entirely in a short-lived
    /// HttpOnly cookie. No server-side storage needed. Default.
    /// </summary>
    Cookie,

    /// <summary>
    /// Server-side, single-instance in-memory cache. True single-use (delete-on-consume).
    /// </summary>
    InMemory,

    /// <summary>
    /// Server-side distributed cache. True single-use (delete-on-consume). Required for
    /// multi-replica deployments.
    /// </summary>
    Distributed,

    /// <summary>
    /// A custom <see cref="OAuthFlow.IJobMasterOAuthFlowStateStorage"/> implementation is in use,
    /// set automatically by <see cref="Ioc.Selectors.Auth.IJobMasterDashboardOAuthSelector.UseCustomStorage{T}"/>.
    /// </summary>
    Custom
}
