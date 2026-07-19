using System.Text.Json;

namespace JobMaster.ScenarioTests.Runner;

public interface IScenarioApiClient
{
    Task<ApiJob?> GetJobAsync(string clusterId, Guid jobId, string? bearerToken = null, CancellationToken ct = default);

    Task<List<ApiJobExecution>> GetJobExecutionsAsync(string clusterId, Guid jobId, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>Returns every ClusterId registered in this api container (not scoped to one cluster).</summary>
    Task<List<string>> GetClusterIdsAsync(string? bearerToken = null, CancellationToken ct = default);

    /// <summary>
    /// Job count for a cluster, optionally filtered to one handler's JobDefinitionId, one
    /// JobMasterPriority (raw int, VeryLow=1 .. Critical=5), one JobMasterJobStatus (raw int,
    /// PendingSave=1 .. Aborted=9), and/or one TestIdentifier (matched via the API's metadata
    /// filter, since TestIdentifier lives in job Metadata, not a first-class column). Any
    /// combination of these can be omitted -- e.g. clusterId alone, or clusterId + testIdentifier
    /// alone, without the others.
    /// </summary>
    Task<int> GetJobCountAsync(string clusterId, string? jobDefinitionId = null, int? priority = null, string? testIdentifier = null, int? status = null, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>
    /// Job list for a cluster, with the same optional filters as <see cref="GetJobCountAsync"/>.
    /// Pass countLimit: int.MaxValue to bypass the API's default 25-item page size and get
    /// everything in one call.
    /// </summary>
    Task<List<ApiJob>> GetJobsAsync(string clusterId, string? jobDefinitionId = null, int? priority = null, string? testIdentifier = null, int? status = null, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>
    /// Mints a JWT via the api container's own /auth/token endpoint (only available when the
    /// scenario's api.json has auth.enableJwt = true). The returned token can be passed as
    /// bearerToken to the other methods above to exercise the JWT auth path.
    /// </summary>
    Task<string> GetJwtTokenAsync(string subject, CancellationToken ct = default);

    /// <summary>
    /// Agent connections registered for a cluster. Pass countLimit: int.MaxValue to bypass the
    /// API's default 25-item page size. <see cref="ApiAgentConnection.IsAlive"/> reflects whether
    /// the connection has heartbeated within JobMaster's ResourceAliveThreshold (~45s) -- it goes
    /// false quickly once nothing uses the connection anymore, well before the separate (much
    /// longer, non-configurable) 30-minute threshold that would physically delete the row.
    /// </summary>
    Task<List<ApiAgentConnection>> GetAgentConnectionsAsync(string clusterId, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>Bucket count for a cluster, optionally filtered to one agent connection, one
    /// BucketStatus (raw int, Active=1 .. ReadyToDelete=6), and/or a specific set of bucket IDs --
    /// used to confirm a real drain has actually finished destroying every bucket for that
    /// connection specifically, not just gone idle cluster-wide (other connections may still own
    /// live buckets), and to see which lifecycle stage any still-undestroyed buckets are stuck in.
    /// The bucketIds filter is what makes this precise once new buckets can exist alongside old
    /// ones on the same connection (e.g. after live executors return) -- count by specific ID
    /// instead of by connection-wide total, which would include the new buckets too.</summary>
    Task<int> GetBucketCountAsync(string clusterId, string? agentConnectionId = null, int? status = null, IEnumerable<string>? bucketIds = null, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>Buckets for a cluster, optionally filtered to one agent connection. Pass
    /// countLimit: int.MaxValue to bypass the API's default 25-item page size. Used to capture the
    /// exact set of bucket IDs that exist at a point in time (e.g. right at the start of a drain,
    /// before any new buckets could exist), so a later phase can assert precisely those buckets --
    /// and no others -- were fully destroyed.</summary>
    Task<List<ApiBucket>> GetBucketsAsync(string clusterId, string? agentConnectionId = null, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>Agent workers for a cluster. Pass countLimit: int.MaxValue to bypass the API's
    /// default 25-item page size.</summary>
    Task<List<ApiAgentWorker>> GetAgentWorkersAsync(string clusterId, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>
    /// Recurring schedules for a cluster, optionally filtered by TestIdentifier (matched via the
    /// API's metadata filter, same rationale as <see cref="GetJobsAsync"/>'s testIdentifier param --
    /// TestIdentifier lives in schedule Metadata, not a first-class column). Pass
    /// countLimit: int.MaxValue to bypass the API's default 25-item page size.
    /// </summary>
    Task<List<ApiRecurringSchedule>> GetRecurringSchedulesAsync(string clusterId, string? testIdentifier = null, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default);
}

// Status/Priority/Outcome are JsonElement (not typed enums) because JobMaster.Api serializes
// its enums as raw numbers by default, and this project must not take a JobMaster reference just
// to share the enum type.
public sealed record ApiJob(string Id, JsonElement Status, string JobDefinitionId, JsonElement Priority);

public sealed record ApiJobExecution(string Id, string JobId, DateTime StartedAt, JsonElement Outcome);

public sealed record ApiAgentConnection(string Id, string Name, bool IsAlive);

public sealed record ApiBucket(string Id, string AgentConnectionId, string AgentConnectionName, JsonElement Status);

public sealed record ApiAgentWorker(
    string Id,
    string Name,
    string? AgentConnectionId,
    string? AgentConnectionName,
    bool IsAlive,
    DateTime? StopRequestedAt,
    DateTime LastHeartbeat,
    JsonElement Mode,
    JsonElement Status);

public sealed record ApiRecurringSchedule(
    string Id,
    string Expression,
    string ExpressionTypeId,
    string JobDefinitionId,
    JsonElement Status,
    JsonElement RecurringScheduleType,
    DateTime CreatedAt,
    DateTime? LastPlanCoverageUntil);
