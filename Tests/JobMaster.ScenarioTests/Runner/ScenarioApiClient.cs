using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
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

    public async Task<int> GetJobCountAsync(string clusterId, string? jobDefinitionId = null, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = BuildJobQuery(jobDefinitionId, countLimit: null);
        using var request = CreateRequest(HttpMethod.Get, $"{basePath}/{clusterId}/jobs/count{query}", bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<int>(ScenarioJsonOptions.Default, ct);
    }

    public async Task<List<ApiJob>> GetJobsAsync(string clusterId, string? jobDefinitionId = null, int countLimit = int.MaxValue, string? bearerToken = null, CancellationToken ct = default)
    {
        var query = BuildJobQuery(jobDefinitionId, countLimit);
        using var request = CreateRequest(HttpMethod.Get, $"{basePath}/{clusterId}/jobs{query}", bearerToken);
        var response = await httpClient.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<List<ApiJob>>(ScenarioJsonOptions.Default, ct);
        return result ?? [];
    }

    public async Task<string> GetJwtTokenAsync(string subject, CancellationToken ct = default)
    {
        var response = await httpClient.PostAsJsonAsync("/auth/token", new { subject }, ct);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<JwtTokenResponse>(ScenarioJsonOptions.Default, ct);
        return result?.Token ?? throw new InvalidOperationException("JWT token response deserialized to null.");
    }

    private static string BuildJobQuery(string? jobDefinitionId, int? countLimit)
    {
        var query = HttpUtility.ParseQueryString(string.Empty);
        if (!string.IsNullOrEmpty(jobDefinitionId))
        {
            query["JobDefinitionId"] = jobDefinitionId;
        }

        if (countLimit.HasValue)
        {
            query["CountLimit"] = countLimit.Value.ToString();
        }

        var queryString = query.ToString();
        return string.IsNullOrEmpty(queryString) ? string.Empty : $"?{queryString}";
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
