using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.DrainModeTest;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioMatrixTests.Scenarios.DrainModeTest;

/// <summary>
/// No job is lost or run twice when every executor stops right after a large schedule, with jobs caught at every
/// stage (pending save on the agent, on master, in a bucket, running): drainers on the same agent connections recover
/// them, then returning executors run them all. Run with both stop types. See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class DrainModeTests(ScenarioGlobalEnvironment global)
{
    private const int SucceededStatus = 5;
    private const int ReadyToDeleteStatus = 6;
    private static readonly string[] AgentConnectionNames = ["agent-1", "agent-2", "agent-3"];
    private static readonly string[] Executors = ["executor-1", "executor-2", "executor-3"];

    // Phase 2 waits only for every job to be on master, whatever its status.
    private static readonly TimeSpan AllJobsSavedTimeout = TimeSpan.FromMinutes(10);

    // Phase 3: the new executors onboard and run the backlog while the drainers are still busy with the old buckets.
    private static readonly TimeSpan FinalizeTimeout = TimeSpan.FromMinutes(45);

    // Phase 1's buckets reaching ReadyToDelete (Lost -> Draining -> the 10 min no-jobs wait), mostly elapsed during
    // FinalizeTimeout. Physical destruction isn't waited for (covered by DestroyReadyToDeleteBucketsRunnerTests).
    private static readonly TimeSpan OldBucketsResolvedTimeout = TimeSpan.FromMinutes(20);

    public static IEnumerable<object[]> Variants() =>
        ScenarioFlavors.AllExcept()
            .SelectMany(flavor => ScenarioStopTypes.All.Select(stopType => new object[] { flavor.Name, stopType }));

    [Theory]
    [MemberData(nameof(Variants))]
    public async Task Run(string flavor, ScenarioStopType stopType)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "DrainModeTest");

        // Per stop type: its own cluster and master database, so the two runs on one flavor never count each other's jobs.
        var stop = stopType == ScenarioStopType.Crash ? "crash" : "sig";
        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json.Replace("{{stop}}", stop), selected));

        var prefix = $"mx-{selected.Code}-{stop}-drain";
        var clusterId = prefix;
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        // Same sizes and priorities as the regular suite, with identifiers unique to this run: every run in this
        // process shares one Redis.
        var runId = Guid.NewGuid().ToString("N")[..8];
        var batches = DrainModeTestPlan.Batches
            .Select(b => b with { TestIdentifier = $"{b.TestIdentifier}-{runId}" })
            .ToList();
        var totalJobs = batches.Sum(b => b.QtyJobs);

        // Phase 1: schedule everything, then stop every executor at once, without waiting for any job to settle, so
        // jobs are caught at every stage of the pipeline and both drain runners (save pending, processing) have work.
        // Each batch is split evenly over the executors, round-robin: each declares only its own agent connection, so
        // this gives every connection its share of pending saves to drain, not just agent-1's.
        await runner.StartPhaseAsync("phase-1");
        var request = 0;
        foreach (var batch in batches)
        {
            var perRequest = Math.Min(DrainModeTestPlan.MaxJobsPerRequest, (batch.QtyJobs + Executors.Length - 1) / Executors.Length);
            for (var sent = 0; sent < batch.QtyJobs; sent += perRequest)
            {
                var executor = Executors[request++ % Executors.Length];
                await runner.ScheduleFor($"{prefix}-{executor}")
                    .ScheduleAsync(batch.HandlerType, batch.TestIdentifier,
                        qtyJobs: Math.Min(perRequest, batch.QtyJobs - sent), clusterId: clusterId, priority: batch.Priority);
            }
        }

        foreach (var executor in Executors)
        {
            await runner.StopAsync($"{prefix}-{executor}", stopType);
        }

        // Phase 2: a coordinator plus one drainer per agent connection, which inherits its dead executor's buckets.
        await runner.StartPhaseAsync("phase-2");

        var connections = (await api.GetAgentConnectionsAsync(clusterId)).ExcludingReserved().ToList();
        connections.Should().HaveCount(3);
        connections.Should().OnlyContain(c => c.IsAlive);
        connections.Select(c => c.Name).Should().BeEquivalentTo(AgentConnectionNames);

        // Phase 1's buckets, captured before anything else can create one on these connections: phase 3 checks
        // exactly these are retired, apart from the new executors' own buckets.
        var originalBucketIds = new List<string>();
        foreach (var name in AgentConnectionNames)
        {
            var buckets = await api.GetBucketsAsync(clusterId, agentConnectionId: connections.Single(c => c.Name == name).Id);
            originalBucketIds.AddRange(buckets.Select(b => b.Id));
        }

        originalBucketIds.Should().NotBeEmpty("phase 1's executors must have created buckets before stopping");

        // No job lost by the stop: every one is on master, whatever its status.
        await PollingWaitUtil.WaitUntilAsync(AllJobsSavedTimeout,
            async () => await api.GetJobCountAsync(clusterId) == totalJobs,
            "every job to be durably saved in the master store",
            pollInterval: TimeSpan.FromSeconds(5),
            describeLastState: async () => $"master has {await api.GetJobCountAsync(clusterId)} of {totalJobs} jobs");

        // Phase 3: executors return on their own connections (the drainers keep running) and run everything left.
        await runner.StartPhaseAsync("phase-3");

        connections = (await api.GetAgentConnectionsAsync(clusterId)).ExcludingReserved().ToList();
        connections.Should().HaveCount(3);
        connections.Should().OnlyContain(c => AgentConnectionNames.Contains(c.Name) && c.IsAlive);

        // Every job saved, run exactly once: none lost, none run twice.
        foreach (var batch in batches)
        {
            var executions = await runner.Tracker.WaitForAsync(batch.TestIdentifier, batch.QtyJobs, FinalizeTimeout);
            executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

            var jobDefinitionId = DrainModeTestPlan.HandlerTypeToJobDefinitionId[batch.HandlerType];
            var apiJobs = await api.GetJobsAsync(clusterId, jobDefinitionId, batch.Priority, batch.TestIdentifier, status: SucceededStatus, countLimit: int.MaxValue);

            apiJobs.Should().HaveCount(batch.QtyJobs);
            new HashSet<Guid>(apiJobs.Select(j => GuidBase64.Parse(j.Id))).SetEquals(executions.Select(e => e.JobId)).Should().BeTrue(
                "the Redis-tracked executions and the API's Succeeded jobs must be exactly the same set -- no job lost, none run twice");
        }

        (await api.GetJobCountAsync(clusterId, status: SucceededStatus)).Should().Be(totalJobs);
        (await api.GetJobCountAsync(clusterId)).Should().Be(totalJobs);

        await DrainModeFailureReport.WriteAsync(api, clusterId);

        // Phase 1's buckets each reach ReadyToDelete or are already gone, by captured id (so the new executors'
        // buckets on the same connections don't count).
        await PollingWaitUtil.WaitUntilAsync(OldBucketsResolvedTimeout,
            async () =>
            {
                var stillExisting = await api.GetBucketCountAsync(clusterId, bucketIds: originalBucketIds);
                var readyToDelete = await api.GetBucketCountAsync(clusterId, bucketIds: originalBucketIds, status: ReadyToDeleteStatus);
                return stillExisting == readyToDelete;
            },
            "every one of phase 1's buckets to have reached ReadyToDelete (or already been destroyed)",
            pollInterval: TimeSpan.FromSeconds(30));
    }
}
