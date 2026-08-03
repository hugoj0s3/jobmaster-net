using System.Net.Http.Json;

namespace JobMaster.Benchmarks.Common.Scheduling;

public sealed class HttpScheduleClient(HttpClient http) : IScheduleClient
{
    public async Task<IReadOnlyList<Guid>> ScheduleNowAsync(int count, CancellationToken ct = default)
    {
        var response = await http.PostAsJsonAsync("/schedule-now", new ScheduleNowRequest(count), ct);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadFromJsonAsync<ScheduleResponse>(cancellationToken: ct);
        return body?.JobIds ?? [];
    }

    public async Task<IReadOnlyList<Guid>> ScheduleAfterAsync(int count, TimeSpan delay, CancellationToken ct = default)
    {
        var response = await http.PostAsJsonAsync("/schedule-after", new ScheduleAfterRequest(count, delay.TotalMinutes), ct);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadFromJsonAsync<ScheduleResponse>(cancellationToken: ct);
        return body?.JobIds ?? [];
    }
}
