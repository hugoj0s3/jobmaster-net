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

        var response = await httpClient.DeleteAsync(path, ct);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync(ct);
            throw new HttpRequestException(
                $"DELETE {path} failed with {(int)response.StatusCode} {response.StatusCode}: {errorBody}");
        }
    }
}
