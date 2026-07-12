using System.Text.Json;

namespace JobMaster.ScenarioTests.Runner;

public interface IScenarioApiClient
{
    Task<ApiJob?> GetJobAsync(string clusterId, Guid jobId, string? bearerToken = null, CancellationToken ct = default);

    Task<List<ApiJobExecution>> GetJobExecutionsAsync(string clusterId, Guid jobId, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>Returns every ClusterId registered in this api container (not scoped to one cluster).</summary>
    Task<List<string>> GetClusterIdsAsync(string? bearerToken = null, CancellationToken ct = default);

    /// <summary>Job count for a cluster, optionally filtered to one handler's JobDefinitionId.</summary>
    Task<int> GetJobCountAsync(string clusterId, string? jobDefinitionId = null, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>
    /// Job list for a cluster, optionally filtered to one handler's JobDefinitionId. Pass
    /// countLimit: int.MaxValue to bypass the API's default 25-item page size and get everything
    /// in one call.
    /// </summary>
    Task<List<ApiJob>> GetJobsAsync(string clusterId, string? jobDefinitionId = null, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default);

    /// <summary>
    /// Mints a JWT via the api container's own /auth/token endpoint (only available when the
    /// scenario's api.json has auth.enableJwt = true). The returned token can be passed as
    /// bearerToken to the other methods above to exercise the JWT auth path.
    /// </summary>
    Task<string> GetJwtTokenAsync(string subject, CancellationToken ct = default);
}

// Status/Outcome are JsonElement (not a typed enum) because JobMaster.Api serializes
// its status/outcome enums as raw numbers by default, and this project must not take a
// JobMaster reference just to share the enum type.
public sealed record ApiJob(string Id, JsonElement Status, string JobDefinitionId);

public sealed record ApiJobExecution(string Id, string JobId, DateTime StartedAt, JsonElement Outcome);
