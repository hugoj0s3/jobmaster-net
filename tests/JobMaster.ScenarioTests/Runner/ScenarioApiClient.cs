using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Web;

namespace JobMaster.ScenarioTests.Runner;

public sealed class ScenarioApiClient(HttpClient httpClient, string basePath = "/jm-api") : IScenarioApiClient
{
    public async Task<ApiJob?> GetJobAsync(string clusterId, Guid jobId, string? bearerToken = null, CancellationToken ct = default)
    {
        using var request = CreateRequest(HttpMethod.Get, $"{basePath}/{clusterId}/jobs/{jobId}", bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<ApiJob>(ScenarioJsonOptions.Default, ct);
    }

    public async Task<List<ApiJobExecution>> GetJobExecutionsAsync(string clusterId, Guid jobId, string? bearerToken = null, CancellationToken ct = default)
    {
        using var request = CreateRequest(HttpMethod.Get, $"{basePath}/{clusterId}/jobs/{jobId}/executions", bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<List<ApiJobExecution>>(ScenarioJsonOptions.Default, ct);
        return result ?? new List<ApiJobExecution>();
    }

    public async Task<List<string>> GetClusterIdsAsync(string? bearerToken = null, CancellationToken ct = default)
    {
        using var request = CreateRequest(HttpMethod.Get, $"{basePath}/clusters/ids", bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<List<string>>(ScenarioJsonOptions.Default, ct);
        return result ?? new List<string>();
    }

    public async Task<int> GetJobCountAsync(string clusterId, string? jobDefinitionId = null, int? priority = null, string? testIdentifier = null, int? status = null, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = BuildJobQuery(jobDefinitionId, priority, testIdentifier, status, countLimit: null);
        var requestUri = $"{basePath}/{clusterId}/jobs/count{query}";
        using var request = CreateRequest(HttpMethod.Get, requestUri, bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, requestUri, ct);

        return await response.Content.ReadFromJsonAsync<int>(ScenarioJsonOptions.Default, ct);
    }

    public async Task<List<ApiJob>> GetJobsAsync(string clusterId, string? jobDefinitionId = null, int? priority = null, string? testIdentifier = null, int? status = null, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = BuildJobQuery(jobDefinitionId, priority, testIdentifier, status, countLimit);
        var requestUri = $"{basePath}/{clusterId}/jobs{query}";
        using var request = CreateRequest(HttpMethod.Get, requestUri, bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, requestUri, ct);

        var result = await response.Content.ReadFromJsonAsync<List<ApiJob>>(ScenarioJsonOptions.Default, ct);
        return result ?? [];
    }

    public async Task<List<ApiAgentConnection>> GetAgentConnectionsAsync(string clusterId, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = HttpUtility.ParseQueryString(string.Empty);
        query["CountLimit"] = countLimit.ToString();
        var requestUri = $"{basePath}/{clusterId}/agent-connections?{query}";
        using var request = CreateRequest(HttpMethod.Get, requestUri, bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, requestUri, ct);

        var result = await response.Content.ReadFromJsonAsync<List<ApiAgentConnection>>(ScenarioJsonOptions.Default, ct);
        return result ?? [];
    }

    public async Task<List<ApiAgentWorker>> GetAgentWorkersAsync(string clusterId, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = HttpUtility.ParseQueryString(string.Empty);
        query["CountLimit"] = countLimit.ToString();
        var requestUri = $"{basePath}/{clusterId}/workers?{query}";
        using var request = CreateRequest(HttpMethod.Get, requestUri, bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, requestUri, ct);

        var result = await response.Content.ReadFromJsonAsync<List<ApiAgentWorker>>(ScenarioJsonOptions.Default, ct);
        return result ?? [];
    }

    public async Task<int> GetBucketCountAsync(string clusterId, string? agentConnectionId = null, int? status = null, IEnumerable<string>? bucketIds = null, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = HttpUtility.ParseQueryString(string.Empty);
        if (!string.IsNullOrEmpty(agentConnectionId))
        {
            query["AgentConnectionId"] = agentConnectionId;
        }

        if (status.HasValue)
        {
            query["Status"] = status.Value.ToString();
        }

        if (bucketIds != null)
        {
            foreach (var bucketId in bucketIds)
            {
                query.Add("BucketIds", bucketId);
            }
        }

        var queryString = query.ToString();
        var requestUri = $"{basePath}/{clusterId}/buckets/count" + (string.IsNullOrEmpty(queryString) ? "" : $"?{queryString}");
        using var request = CreateRequest(HttpMethod.Get, requestUri, bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, requestUri, ct);

        return await response.Content.ReadFromJsonAsync<int>(ScenarioJsonOptions.Default, ct);
    }

    public async Task<List<ApiBucket>> GetBucketsAsync(string clusterId, string? agentConnectionId = null, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = HttpUtility.ParseQueryString(string.Empty);
        if (!string.IsNullOrEmpty(agentConnectionId))
        {
            query["AgentConnectionId"] = agentConnectionId;
        }

        query["CountLimit"] = countLimit.ToString();
        var requestUri = $"{basePath}/{clusterId}/buckets?{query}";
        using var request = CreateRequest(HttpMethod.Get, requestUri, bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, requestUri, ct);

        var result = await response.Content.ReadFromJsonAsync<List<ApiBucket>>(ScenarioJsonOptions.Default, ct);
        return result ?? [];
    }

    public async Task<List<ApiRecurringSchedule>> GetRecurringSchedulesAsync(string clusterId, string? testIdentifier = null, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = HttpUtility.ParseQueryString(string.Empty);
        if (!string.IsNullOrEmpty(testIdentifier))
        {
            // TestIdentifier lives in schedule Metadata, not a first-class queryable column, same
            // rationale as BuildJobQuery's testIdentifier handling above.
            var metadataFilters = new[] { new { Key = "TestIdentifier", Operation = "Eq", Value = testIdentifier } };
            query["MetadataFiltersJson"] = JsonSerializer.Serialize(metadataFilters);
        }

        query["CountLimit"] = countLimit.ToString();
        var requestUri = $"{basePath}/{clusterId}/recurring-schedules?{query}";
        using var request = CreateRequest(HttpMethod.Get, requestUri, bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, requestUri, ct);

        var result = await response.Content.ReadFromJsonAsync<List<ApiRecurringSchedule>>(ScenarioJsonOptions.Default, ct);
        return result ?? [];
    }

    public async Task<List<ApiLogItem>> GetLogsAsync(string clusterId, Guid referenceId, int? category = null, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = HttpUtility.ParseQueryString(string.Empty);
        // ReferenceGuid (not ReferenceId) -- the API normalises it to the "N" (no-dashes) format logs
        // are actually stored under (see JobMasterLoggerExtensions), so callers don't need to know that.
        query["ReferenceGuid"] = referenceId.ToString();
        if (category.HasValue)
        {
            query["Category"] = category.Value.ToString();
        }

        query["CountLimit"] = countLimit.ToString();
        var requestUri = $"{basePath}/{clusterId}/logs?{query}";
        using var request = CreateRequest(HttpMethod.Get, requestUri, bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, requestUri, ct);

        var result = await response.Content.ReadFromJsonAsync<List<ApiLogItem>>(ScenarioJsonOptions.Default, ct);
        return result ?? [];
    }

    public async Task<string> GetJwtTokenAsync(string subject, CancellationToken ct = default)
    {
        var response = await httpClient.PostAsJsonAsync("/auth/token", new { subject }, ct);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<JwtTokenResponse>(ScenarioJsonOptions.Default, ct);
        return result?.Token ?? throw new InvalidOperationException("JWT token response deserialized to null.");
    }

    private static string BuildJobQuery(string? jobDefinitionId, int? priority, string? testIdentifier, int? status, int? countLimit)
    {
        var query = HttpUtility.ParseQueryString(string.Empty);
        if (!string.IsNullOrEmpty(jobDefinitionId))
        {
            query["JobDefinitionId"] = jobDefinitionId;
        }

        if (priority.HasValue)
        {
            query["Priority"] = priority.Value.ToString();
        }

        if (status.HasValue)
        {
            query["Status"] = status.Value.ToString();
        }

        if (!string.IsNullOrEmpty(testIdentifier))
        {
            // TestIdentifier lives in job Metadata, not a first-class queryable column, so it's
            // matched via the API's generic metadata filter mechanism instead of a dedicated param.
            var metadataFilters = new[] { new { Key = "TestIdentifier", Operation = "Eq", Value = testIdentifier } };
            query["MetadataFiltersJson"] = JsonSerializer.Serialize(metadataFilters);
        }

        if (countLimit.HasValue)
        {
            query["CountLimit"] = countLimit.Value.ToString();
        }

        var queryString = query.ToString();
        return string.IsNullOrEmpty(queryString) ? string.Empty : $"?{queryString}";
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, string requestUri, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var errorBody = await response.Content.ReadAsStringAsync(ct);
        throw new HttpRequestException(
            $"GET {requestUri} failed with {(int)response.StatusCode} {response.StatusCode}: {errorBody}");
    }

    private static HttpRequestMessage CreateRequest(HttpMethod method, string requestUri, string? bearerToken)
    {
        var request = new HttpRequestMessage(method, requestUri);
        if (!string.IsNullOrEmpty(bearerToken))
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
        }

        return request;
    }

    private sealed record JwtTokenResponse(string Token);
}
