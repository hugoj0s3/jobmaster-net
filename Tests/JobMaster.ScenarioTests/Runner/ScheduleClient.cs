using System.Net.Http.Json;

namespace JobMaster.ScenarioTests.Runner;

public sealed class ScheduleClient(HttpClient httpClient) : IScheduleClient
{
    public async Task<ScheduleClientResult> ScheduleAsync(
        string handlerType,
        string testIdentifier,
        int qtyJobs = 1,
        string? clusterId = null,
        int? afterSeconds = null,
        CancellationToken ct = default)
    {
        var body = new
        {
            ClusterId = clusterId,
            QtyJobs = qtyJobs,
            AfterSeconds = afterSeconds,
            TestIdentifier = testIdentifier
        };

        var response = await httpClient.PostAsJsonAsync($"/schedule/{handlerType}", body, ct);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<ScheduleClientResult>(ScenarioJsonOptions.Default, ct);
        return result ?? throw new InvalidOperationException("Schedule response deserialized to null.");
    }
}
