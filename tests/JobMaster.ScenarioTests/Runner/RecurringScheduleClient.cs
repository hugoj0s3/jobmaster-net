using System.Net.Http.Json;

namespace JobMaster.ScenarioTests.Runner;

public sealed class RecurringScheduleClient(HttpClient httpClient) : IRecurringScheduleClient
{
    public async Task<RecurringScheduleClientResult> CreateRecurringAsync(
        string handlerType,
        string expressionTypeId,
        string expression,
        string testIdentifier,
        string? clusterId = null,
        CancellationToken ct = default)
    {
        var body = new
        {
            ClusterId = clusterId,
            ExpressionTypeId = expressionTypeId,
            Expression = expression,
            TestIdentifier = testIdentifier
        };

        // Warm the connection on a safe, idempotent request first -- this HttpClient is freshly
        // constructed per RecurringScheduleFor(...) call, and a cold first connection to a container
        // that just passed its health check can still hit a transient reset that HttpClient retries
        // transparently. That retry is safe for GET but not for this POST, which is why duplicate
        // recurring schedules were observed intermittently without it.
        await httpClient.GetAsync("/health", ct);

        var response = await httpClient.PostAsJsonAsync($"/recurring-schedule/{handlerType}", body, ct);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync(ct);
            throw new HttpRequestException(
                $"POST /recurring-schedule/{handlerType} failed with {(int)response.StatusCode} {response.StatusCode}: {errorBody}");
        }

        var result = await response.Content.ReadFromJsonAsync<RecurringScheduleClientResult>(ScenarioJsonOptions.Default, ct);
        return result ?? throw new InvalidOperationException("Recurring schedule response deserialized to null.");
    }

    public async Task CancelRecurringAsync(Guid recurringScheduleId, string? clusterId = null, CancellationToken ct = default)
    {
        var path = $"/recurring-schedule/{recurringScheduleId}";
        if (!string.IsNullOrEmpty(clusterId))
        {
            path += $"?clusterId={Uri.EscapeDataString(clusterId)}";
        }

        // Same cold-connection rationale as CreateRecurringAsync -- this HttpClient is freshly
        // constructed per RecurringScheduleFor(...) call, independent of whichever client issued the
        // create.
        await httpClient.GetAsync("/health", ct);

        var response = await httpClient.DeleteAsync(path, ct);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync(ct);
            throw new HttpRequestException(
                $"DELETE {path} failed with {(int)response.StatusCode} {response.StatusCode}: {errorBody}");
        }
    }
}
