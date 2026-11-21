using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioMatrixTests.Scenarios.DrainModeTest;

/// <summary>
/// Diagnostic, not asserted yet: failed attempts counted by the stop (e.g. ObjectDisposedException after the host
/// disposed the app's container, or a handler aborted by the stop), although the jobs ran again and succeeded.
/// Written to jm-scenario-diag/drainmode-failures-{clusterId}.txt and printed.
/// </summary>
internal static class DrainModeFailureReport
{
    public static async Task WriteAsync(IScenarioApiClient api, string clusterId)
    {
        var jobsWithFailures = (await api.GetJobsAsync(clusterId, countLimit: int.MaxValue))
            .Where(j => j.NumberOfFailures > 0)
            .ToList();

        var messages = new List<string>();
        foreach (var job in jobsWithFailures)
        {
            var executions = await api.GetJobExecutionsAsync(clusterId, GuidBase64.Parse(job.Id));
            messages.AddRange(executions
                .Where(e => e.OutcomeMessage != null && !e.Outcome.ToString().Contains("Succeeded", StringComparison.OrdinalIgnoreCase))
                .Select(e => e.OutcomeMessage!));
        }

        // Jobs that hit the disposed container before starting (not counted as failures, so invisible above).
        var notStarted = await api.SearchLogsAsync(clusterId, "the app's DI container is disposed");

        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"cluster={clusterId} utcNow={DateTime.UtcNow:O} jobsWithFailures={jobsWithFailures.Count} failedAttempts={messages.Count}");
        sb.AppendLine($"Not started, app container disposed (Warn): {notStarted.Count}");
        sb.AppendLine($"ObjectDisposedException: {messages.Count(m => m.Contains("ObjectDisposedException"))}");
        sb.AppendLine($"Timed out: {messages.Count(m => m.Contains("Timed out"))}");
        foreach (var group in messages.GroupBy(m => m.Length > 160 ? m[..160] : m).OrderByDescending(g => g.Count()))
        {
            sb.AppendLine($"{group.Count(),5} × {group.Key}");
        }

        var dir = Path.Combine(Path.GetTempPath(), "jm-scenario-diag");
        Directory.CreateDirectory(dir);
        await File.WriteAllTextAsync(Path.Combine(dir, $"drainmode-failures-{clusterId}.txt"), sb.ToString());
        Console.WriteLine(sb.ToString());
    }
}
