using System.Text;
using System.Text.Json;
using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioMatrixTests.Lib;

namespace JobMaster.ScenarioMatrixTests.Scenarios.DataRetentionArchiveTest;

/// <summary>
/// Finished jobs and terminated recurring schedules on a cluster with <c>DataRetentionTtl</c> and
/// <c>TargetArchivedClusterId</c> are copied to the archive cluster intact (same ids, same final status, their
/// executions and failure logs too), then removed from the source. One container hosts both clusters (the purge
/// runners forward through <c>JobMasterClusterAwareComponentFactories.TryGetFactory</c>, same process only).
/// See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class DataRetentionArchiveTests(ScenarioGlobalEnvironment global)
{
    private const string TimeSpanIntervalExpressionTypeId = "TimeSpanInterval";
    private const string RecurringInterval = "00:06:00";
    private const int QtyJobs = 200;
    private const int SucceededStatus = 5;
    private const int FailedStatus = 7;
    private const int CanceledStatus = 3;
    // ApiJobExecution.Outcome is the enum's name (unlike Status/Priority/Category, raw ints).
    private const string SucceededOutcome = "Succeeded";
    private const string FailedOutcome = "Failed";
    private const int JobExecutionLogCategory = 2;
    private const int ExecutionSampleSize = 5;

    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(10);

    // DataRetentionTtl is the 10 min floor; the purge runners poll every Clamp(TTL/2, 5 min, 1 h) = 5 min here.
    private static readonly TimeSpan ArchiveWaitTimeout = TimeSpan.FromMinutes(20);

    public static IEnumerable<object[]> Flavors() =>
        ScenarioFlavors.AllExcept().Select(flavor => new object[] { flavor.Name });

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Run(string flavor)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "DataRetentionArchiveTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));
        await runner.StartPhaseAsync("phase-1");

        var sourceClusterId = $"mx-{selected.Code}-arch-source";
        var targetClusterId = $"mx-{selected.Code}-arch-target";
        var containerName = $"mx-{selected.Code}-archive";
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        var testIdentifier = Guid.NewGuid().ToString("N");
        var recurringTestIdentifier = Guid.NewGuid().ToString("N");

        // One job fails its only attempt (no retries, so it's Failed at once), so a Failed execution and its
        // JobExecution-category log (written only on a failed attempt) are archived too, not just the happy path.
        var scheduled = await runner.ScheduleFor(containerName)
            .ScheduleAsync("fast", testIdentifier, QtyJobs - 1, clusterId: sourceClusterId);
        var failureInjected = await runner.ScheduleFor(containerName)
            .ScheduleAsync("fast", testIdentifier, qtyJobs: 1, clusterId: sourceClusterId, injectFailure: true, maxNumberOfRetries: 0);
        var allJobIds = scheduled.JobIds.Concat(failureInjected.JobIds).ToList();
        allJobIds.Should().HaveCount(QtyJobs);
        var failedJobId = failureInjected.JobIds.Single();

        var recurring = await runner.RecurringScheduleFor(containerName)
            .CreateRecurringAsync("fast", TimeSpanIntervalExpressionTypeId, RecurringInterval, recurringTestIdentifier, clusterId: sourceClusterId);

        // The source has an agent connection, so the new schedule goes through the agent before it's on master;
        // cancelling it earlier would 404.
        await PollingWaitUtil.WaitUntilAsync(TimeSpan.FromSeconds(30),
            async () => (await api.GetRecurringSchedulesAsync(sourceClusterId, recurringTestIdentifier)).Count == 1,
            "the newly created recurring schedule to be on master before cancelling it",
            pollInterval: TimeSpan.FromMilliseconds(500));

        await runner.RecurringScheduleFor(containerName)
            .CancelRecurringAsync(recurring.RecurringScheduleId, clusterId: sourceClusterId);

        // The failure-injected job never records an execution, so it isn't counted here.
        IReadOnlyList<ExecutionRecord> executions;
        try
        {
            executions = await runner.Tracker.WaitForAsync(testIdentifier, QtyJobs - 1, ExecuteWaitTimeout);
        }
        catch (TimeoutException ex)
        {
            var diagPath = await DumpMissingJobsAsync(runner, api, sourceClusterId, containerName, testIdentifier, scheduled.JobIds);
            throw new TimeoutException($"{ex.Message}{Environment.NewLine}DIAG: {diagPath}", ex);
        }
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        await PollingWaitUtil.WaitUntilAsync(ExecuteWaitTimeout,
            async () => (await api.GetJobAsync(sourceClusterId, failedJobId))?.Status.GetInt32() == FailedStatus,
            $"the failure-injected job {failedJobId} to exhaust its one allowed attempt and reach Failed",
            pollInterval: TimeSpan.FromMilliseconds(500));

        (await api.GetJobCountAsync(sourceClusterId, testIdentifier: testIdentifier, status: SucceededStatus)).Should().Be(QtyJobs - 1);
        (await api.GetJobCountAsync(sourceClusterId, testIdentifier: testIdentifier, status: FailedStatus)).Should().Be(1);

        // Once DataRetentionTtl elapses, DeleteOldFinalJobsRunner archives the finished jobs to the target, then
        // deletes them from the source.
        await PollingWaitUtil.WaitUntilAsync(ArchiveWaitTimeout,
            async () => await api.GetJobCountAsync(sourceClusterId, testIdentifier: testIdentifier) == 0,
            "every finalized job on the Active cluster to be archived and purged after DataRetentionTtl",
            pollInterval: TimeSpan.FromSeconds(30));

        var archivedJobs = await api.GetJobsAsync(targetClusterId, testIdentifier: testIdentifier, countLimit: int.MaxValue);
        archivedJobs.Should().HaveCount(QtyJobs);
        archivedJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(allJobIds,
            "archiving must preserve the exact same job IDs -- the intake service reassigns ClusterId only, it never re-creates the job with a new Id");
        archivedJobs.Where(j => GuidBase64.Parse(j.Id) != failedJobId).Should().OnlyContain(j => j.Status.GetInt32() == SucceededStatus,
            "the archived copy must keep the Succeeded status it had at purge time");
        archivedJobs.Single(j => GuidBase64.Parse(j.Id) == failedJobId).Status.GetInt32().Should().Be(FailedStatus,
            "the archived copy of the failure-injected job must keep the Failed status it had at purge time");

        // Every attempt records an execution: a sample of ordinary jobs kept their single Succeeded one.
        foreach (var jobId in scheduled.JobIds.Take(ExecutionSampleSize))
        {
            var sampleExecutions = await api.GetJobExecutionsAsync(targetClusterId, jobId);
            sampleExecutions.Should().ContainSingle(e => e.Outcome.GetString() == SucceededOutcome,
                $"job {jobId}'s single successful execution attempt must have been archived alongside it");
        }

        var failedJobExecutions = await api.GetJobExecutionsAsync(targetClusterId, failedJobId);
        failedJobExecutions.Should().ContainSingle(e => e.Outcome.GetString() == FailedOutcome,
            "the failure-injected job's Failed execution must have been archived alongside it");

        var failedJobLogs = await api.GetLogsAsync(targetClusterId, failedJobId, category: JobExecutionLogCategory);
        failedJobLogs.Should().NotBeEmpty(
            "the failed attempt's JobExecution-category log must have been archived alongside the job, " +
            "not lost the way DeleteOldLogsRunner's blanket purge would otherwise have raced it away");

        var sourceFailedJobLogs = await api.GetLogsAsync(sourceClusterId, failedJobId, category: JobExecutionLogCategory);
        sourceFailedJobLogs.Should().BeEmpty(
            "the log must have been purged from the source once archived, not merely copied to the target");

        // The same settings drive DeleteOldInactiveRecurringSchedulesRunner: the cancelled schedule is a candidate
        // from the moment it was cancelled.
        await PollingWaitUtil.WaitUntilAsync(ArchiveWaitTimeout,
            async () => (await api.GetRecurringSchedulesAsync(sourceClusterId, recurringTestIdentifier)).Count == 0,
            "the terminated recurring schedule on the Active cluster to be archived and purged after DataRetentionTtl",
            pollInterval: TimeSpan.FromSeconds(30));

        var archivedSchedules = await api.GetRecurringSchedulesAsync(targetClusterId, recurringTestIdentifier);
        archivedSchedules.Should().ContainSingle();
        GuidBase64.Parse(archivedSchedules[0].Id).Should().Be(recurring.RecurringScheduleId);
        archivedSchedules[0].Status.GetInt32().Should().Be(CanceledStatus,
            "the archived copy must keep the Canceled status it had at purge time");
    }

    // On an execution-wait timeout: what happened to the jobs that never ran, written to a file named in the failure.
    private static async Task<string> DumpMissingJobsAsync(
        ScenarioRunner runner, IScenarioApiClient api, string clusterId, string containerName, string testIdentifier, IReadOnlyCollection<Guid> jobIds)
    {
        var sb = new StringBuilder();
        var recorded = (await runner.Tracker.GetAllAsync(testIdentifier)).Select(r => r.JobId).ToHashSet();
        var missing = jobIds.Where(id => !recorded.Contains(id)).ToList();
        sb.AppendLine($"utcNow={DateTime.UtcNow:O} missing={string.Join(",", missing)}");
        foreach (var id in missing)
        {
            sb.AppendLine($"== job {id}");
            sb.AppendLine(JsonSerializer.Serialize(await api.GetJobAsync(clusterId, id)));
            sb.AppendLine("executions: " + JsonSerializer.Serialize(await api.GetJobExecutionsAsync(clusterId, id)));
            foreach (var log in await api.GetLogsAsync(clusterId, id))
            {
                sb.AppendLine($"log {log.TimestampUtc:O} {log.Category}: {log.Message}");
            }
        }

        sb.AppendLine("== buckets");
        sb.AppendLine(JsonSerializer.Serialize(await api.GetBucketsAsync(clusterId)));
        sb.AppendLine("== workers");
        sb.AppendLine(JsonSerializer.Serialize(await api.GetAgentWorkersAsync(clusterId)));
        sb.AppendLine("== container logs");
        sb.AppendLine(await runner.GetContainerLogsAsync(containerName));

        var dir = Path.Combine(Path.GetTempPath(), "jm-scenario-diag");
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, $"{testIdentifier}.txt");
        await File.WriteAllTextAsync(path, sb.ToString());
        return path;
    }
}
