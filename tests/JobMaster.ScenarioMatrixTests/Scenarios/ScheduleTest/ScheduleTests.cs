using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioMatrixTests.Scenarios.ScheduleTest;

/// <summary>
/// Scheduling end to end on three clusters (standalone, two distributed), then the lifecycle around it: data
/// retention purge, a real drain to zero buckets with a control cluster, and retiring the drained connections.
/// See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class ScheduleTests(ScenarioGlobalEnvironment global)
{
    private static readonly TimeSpan DelayAfter = TimeSpan.FromMinutes(5);

    // Mirrors JobMasterJobStatus.Succeeded (no JobMaster reference in this project).
    private const int SucceededStatus = 5;

    // "verylong" (3 min/job) is left out: the standalone cluster has one worker, and a few of those would eat into
    // the 5 min delay window the test relies on.
    private static readonly Dictionary<string, string> HandlerTypeToJobDefinitionId = new()
    {
        ["fast"] = "TestApp.Fast",
        ["normal"] = "TestApp.Normal",
        ["slow"] = "TestApp.Slow",
        // Through IJobMasterScheduler.Advanced.OnceNow/OnceAfter<TDefinition>() (JobDefinitionConfigAttribute path).
        ["advanced-fast"] = "TestApp.AdvancedFast"
    };

    // One distinct priority per handler type (VeryLow=1 .. Critical=5), so the plan can be filtered by priority.
    private static readonly Dictionary<string, int> HandlerTypeToPriority = new()
    {
        ["fast"] = 3,          // Medium
        ["normal"] = 4,        // High
        ["slow"] = 2,          // Low
        ["advanced-fast"] = 5  // Critical
    };

    // Phase 2: DataRetentionTtl is the 10 min floor; the purge runner polls every Clamp(TTL/2, 5 min, 1 h) = 5 min.
    private static readonly TimeSpan PurgeWaitTimeout = TimeSpan.FromMinutes(20);

    // Phase 2: worker-death detection, Lost -> ReadyToDrain, Draining, the 10 min no-jobs wait, then the destroy tick.
    private static readonly TimeSpan DrainWaitTimeout = TimeSpan.FromMinutes(30);

    // Phase 3: connections go dead in ~45 s; CleanupDeadAgentConnectionsRunner deletes one dead for 30 min (fixed),
    // on a 5 min tick.
    private static readonly TimeSpan AliveFlipWaitTimeout = TimeSpan.FromMinutes(3);
    private static readonly TimeSpan ConnectionDeletionWaitTimeout = TimeSpan.FromMinutes(40);

    public static IEnumerable<object[]> Flavors() =>
        ScenarioFlavors.AllExcept().Select(flavor => new object[] { flavor.Name });

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Run(string flavor)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "ScheduleTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));

        var standaloneClusterId = $"mx-{selected.Code}-standalone";
        var drainClusterId = $"mx-{selected.Code}-dist-one";
        var controlClusterId = $"mx-{selected.Code}-dist-two";
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        await runner.StartPhaseAsync("phase-1");
        await RunSchedulingAsync(runner, api, [standaloneClusterId, drainClusterId, controlClusterId]);

        await runner.StartPhaseAsync("phase-2");
        await RunRetentionAndDrainAsync(api, standaloneClusterId, drainClusterId, controlClusterId);

        await runner.StartPhaseAsync("phase-3");
        await RunConnectionRetirementAsync(api, drainClusterId);
    }

    // Phase 1: ~1000 jobs across every cluster, immediate and 5 min delayed, several handler types and priorities.
    // Every cluster's TransientThreshold is 2 min, well under the delay, so a delayed job mustn't run when it becomes
    // threshold-eligible, only at its due time. The plan built up front drives every assertion, in Redis and in the
    // API, filtered by cluster, cluster + handler, cluster + priority, and cluster + test identifier.
    private static async Task RunSchedulingAsync(ScenarioRunner runner, IScenarioApiClient api, IReadOnlyList<string> clusterIds)
    {
        var plan = BuildPlan(clusterIds);

        // Everything is scheduled up front so each delayed batch's 5 min clock starts as early as possible.
        var batches = new List<ScheduledBatch>();
        foreach (var qty in plan)
        {
            var testIdentifier = Guid.NewGuid().ToString("N");
            var batchScheduledAtUtc = DateTime.UtcNow;

            var scheduled = await runner.ScheduleFor(qty.ClusterId)
                .ScheduleAsync(qty.HandlerType, testIdentifier, qtyJobs: qty.QtyJobs, clusterId: qty.ClusterId, afterSeconds: qty.AfterSecs, priority: qty.Priority);
            scheduled.JobIds.Should().HaveCount(qty.QtyJobs);

            batches.Add(new ScheduledBatch(qty, testIdentifier, scheduled.JobIds, batchScheduledAtUtc));
        }

        var immediateBatches = batches.Where(b => b.Plan.AfterSecs is null).ToList();
        var delayedBatches = batches.Where(b => b.Plan.AfterSecs is not null).ToList();

        foreach (var batch in delayedBatches)
        {
            (await runner.Tracker.GetAllAsync(batch.TestIdentifier)).Should().BeEmpty(
                $"delayed batch {batch.Plan.ClusterId}/{batch.Plan.HandlerType} must not execute before its {batch.Plan.AfterSecs}s delay elapses");
        }

        // Immediate jobs finish well within the 5 min window.
        foreach (var batch in immediateBatches)
        {
            var executions = await runner.Tracker.WaitForAsync(batch.TestIdentifier, batch.Plan.QtyJobs, TimeSpan.FromMinutes(3));
            executions.Select(e => e.JobId).Should().BeEquivalentTo(batch.JobIds);
        }

        // Still not run early after all that immediate work.
        foreach (var batch in delayedBatches)
        {
            var elapsed = DateTime.UtcNow - batch.ScheduledAtUtc;
            if (elapsed < DelayAfter)
            {
                (await runner.Tracker.GetAllAsync(batch.TestIdentifier)).Should().BeEmpty(
                    $"delayed batch {batch.Plan.ClusterId}/{batch.Plan.HandlerType} executed early, " +
                    $"{elapsed.TotalSeconds:F0}s after scheduling but before its {batch.Plan.AfterSecs}s delay");
            }
        }

        // Each delayed batch runs once its delay is over, and not before its due time.
        foreach (var batch in delayedBatches)
        {
            var dueAtUtc = batch.ScheduledAtUtc.AddSeconds(batch.Plan.AfterSecs!.Value);
            var remaining = dueAtUtc - DateTime.UtcNow;
            var timeout = (remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero) + TimeSpan.FromMinutes(2);

            var executions = await runner.Tracker.WaitForAsync(batch.TestIdentifier, batch.Plan.QtyJobs, timeout);
            executions.Select(e => e.JobId).Should().BeEquivalentTo(batch.JobIds);
            executions.Min(e => e.ExecutedAtUtc).Should().BeOnOrAfter(dueAtUtc.AddSeconds(-10),
                "a delayed job must not run meaningfully before its due time");
        }

        // Settle, then: each batch ran exactly once.
        await Task.Delay(TimeSpan.FromSeconds(3));
        foreach (var batch in batches)
        {
            var all = await runner.Tracker.GetAllAsync(batch.TestIdentifier);
            all.Should().HaveCount(batch.Plan.QtyJobs);
            all.Select(e => e.JobId).Should().OnlyHaveUniqueItems();
        }

        await AssertApiAsync(api, clusterIds, batches);
    }

    // Phase 2, on the jobs phase 1 finished: the standalone cluster gets DataRetentionTtl (purge); the drain cluster
    // runs a coordinator plus its 3 workers in Drain mode (the only supported way to retire a connection: only a
    // coordinator hands a dead worker's Lost buckets to the drain workers); the control cluster restarts unchanged,
    // proving the TTL causes the purge, not the restart.
    private static async Task RunRetentionAndDrainAsync(IScenarioApiClient api, string ttlClusterId, string drainClusterId, string controlClusterId)
    {
        (await api.GetJobCountAsync(ttlClusterId)).Should().BeGreaterThan(0, "phase 1 should have left completed jobs on this cluster");
        var drainBefore = await api.GetJobCountAsync(drainClusterId);
        var controlBefore = await api.GetJobCountAsync(controlClusterId);
        drainBefore.Should().BeGreaterThan(0, "phase 1 should have left completed jobs on this cluster");
        controlBefore.Should().BeGreaterThan(0, "phase 1 should have left completed jobs on this cluster");

        // The drain workers are up and heartbeating (the coordinator's reserved fallback connection never is).
        var drainConnections = (await api.GetAgentConnectionsAsync(drainClusterId)).ExcludingReserved().ToList();
        drainConnections.Should().NotBeEmpty();
        drainConnections.Should().OnlyContain(c => c.IsAlive);

        await PollingWaitUtil.WaitUntilAsync(PurgeWaitTimeout,
            async () => await api.GetJobCountAsync(ttlClusterId) == 0,
            $"jobs on {ttlClusterId} to be purged after DataRetentionTtl");

        (await api.GetJobCountAsync(controlClusterId)).Should().Be(controlBefore);

        // No TTL on the drain cluster: its jobs stay; what's checked is the real drain destroying every bucket.
        (await api.GetJobCountAsync(drainClusterId)).Should().Be(drainBefore);

        await PollingWaitUtil.WaitUntilAsync(DrainWaitTimeout,
            async () => await api.GetBucketCountAsync(drainClusterId) == 0,
            $"every bucket on {drainClusterId} to be destroyed by the real drain lifecycle");
    }

    // Phase 3: the drain cluster restarts with only its coordinator. A Drain worker keeps heartbeating its connection
    // even with nothing left, so removing them is what lets the connections go idle: dead fast, deleted much later.
    private static async Task RunConnectionRetirementAsync(IScenarioApiClient api, string drainClusterId)
    {
        (await api.GetBucketCountAsync(drainClusterId)).Should().Be(0);

        await PollingWaitUtil.WaitUntilAsync(AliveFlipWaitTimeout,
            async () => (await api.GetAgentConnectionsAsync(drainClusterId)).ExcludingReserved().All(c => !c.IsAlive),
            $"agent connections on {drainClusterId} to go dead once their Drain workers are retired");

        // Deletion needs no buckets left (phase 2 proved it); the reserved fallback connection is never deleted.
        await PollingWaitUtil.WaitUntilAsync(ConnectionDeletionWaitTimeout,
            async () => !(await api.GetAgentConnectionsAsync(drainClusterId)).ExcludingReserved().Any(),
            $"agent connections on {drainClusterId} to be physically deleted after going unused");
    }

    private static List<JobsQty> BuildPlan(IReadOnlyList<string> clusterIds)
    {
        var delaySeconds = (int)DelayAfter.TotalSeconds;
        var plan = new List<JobsQty>();

        // 150/50/3/20 immediate + 100/30/10 delayed per cluster, calibrated for 3 clusters (~1000 jobs); scaled by
        // 3/clusterCount to keep the total near 1000.
        var scale = 3.0 / clusterIds.Count;
        foreach (var clusterId in clusterIds)
        {
            plan.Add(CreateQty(clusterId, "fast", Scale(150, scale)));
            plan.Add(CreateQty(clusterId, "normal", Scale(50, scale)));
            plan.Add(CreateQty(clusterId, "slow", Scale(3, scale)));
            plan.Add(CreateQty(clusterId, "advanced-fast", Scale(20, scale)));

            plan.Add(CreateQty(clusterId, "fast", Scale(100, scale), delaySeconds));
            plan.Add(CreateQty(clusterId, "normal", Scale(30, scale), delaySeconds));
            plan.Add(CreateQty(clusterId, "advanced-fast", Scale(10, scale), delaySeconds));
        }

        return plan;
    }

    private static int Scale(int baseQty, double scale) =>
        (int)Math.Round(baseQty * scale, MidpointRounding.AwayFromZero);

    private static JobsQty CreateQty(string clusterId, string handlerType, int qtyJobs, int? afterSecs = null) => new()
    {
        ClusterId = clusterId,
        HandlerType = handlerType,
        QtyJobs = qtyJobs,
        AfterSecs = afterSecs,
        Priority = HandlerTypeToPriority[handlerType]
    };

    private static async Task AssertApiAsync(IScenarioApiClient api, IReadOnlyList<string> clusterIds, IReadOnlyList<ScheduledBatch> batches)
    {
        foreach (var clusterId in clusterIds)
        {
            var clusterBatches = batches.Where(b => b.Plan.ClusterId == clusterId).ToList();

            // Cluster only.
            await AssertJobsAsync(api, clusterId, clusterBatches.SelectMany(b => b.JobIds).ToHashSet());

            // Cluster + job definition.
            foreach (var handlerType in clusterBatches.Select(b => b.Plan.HandlerType).Distinct())
            {
                var jobDefinitionId = HandlerTypeToJobDefinitionId[handlerType];
                var expected = clusterBatches.Where(b => b.Plan.HandlerType == handlerType).SelectMany(b => b.JobIds).ToHashSet();
                var jobs = await AssertJobsAsync(api, clusterId, expected, jobDefinitionId: jobDefinitionId);
                jobs.Should().OnlyContain(j => j.JobDefinitionId == jobDefinitionId);
            }

            // Cluster + priority.
            foreach (var priority in clusterBatches.Select(b => b.Plan.Priority).Distinct())
            {
                var expected = clusterBatches.Where(b => b.Plan.Priority == priority).SelectMany(b => b.JobIds).ToHashSet();
                var jobs = await AssertJobsAsync(api, clusterId, expected, priority: priority);
                jobs.Should().OnlyContain(j => j.Priority.GetInt32() == priority);
            }

            // Cluster + test identifier (metadata filter), one batch at a time.
            foreach (var batch in clusterBatches)
            {
                await AssertJobsAsync(api, clusterId, batch.JobIds.ToHashSet(), testIdentifier: batch.TestIdentifier);
            }
        }
    }

    private static async Task<List<ApiJob>> AssertJobsAsync(
        IScenarioApiClient api, string clusterId, IReadOnlySet<Guid> expectedJobIds,
        string? jobDefinitionId = null, int? priority = null, string? testIdentifier = null)
    {
        (await api.GetJobCountAsync(clusterId, jobDefinitionId, priority, testIdentifier)).Should().Be(expectedJobIds.Count);

        var jobs = await api.GetJobsAsync(clusterId, jobDefinitionId, priority, testIdentifier, countLimit: int.MaxValue);
        jobs.Should().HaveCount(expectedJobIds.Count);
        jobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(expectedJobIds);
        jobs.Should().OnlyContain(j => j.Status.GetInt32() == SucceededStatus, "every job in this scenario must finish successfully");
        return jobs;
    }

    private sealed record ScheduledBatch(JobsQty Plan, string TestIdentifier, List<Guid> JobIds, DateTime ScheduledAtUtc);
}
