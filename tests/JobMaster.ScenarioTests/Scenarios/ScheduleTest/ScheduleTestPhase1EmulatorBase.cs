using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest;

/// <summary>
/// Shared phase-1 logic for every "Pure" single-repo-type schedule test (PostgresPure, MySqlPure,
/// SqlServerPure, ...): schedules ~1000 jobs across all of the scenario's clusters, mixing
/// immediate and 5-minute-delayed jobs across several handler types and priorities. Every cluster's
/// TransientThreshold is 2 minutes (see each scenario's Phase1/*.json) -- well under the 5-minute
/// delay -- so this also proves a delayed job is not dispatched the moment it's threshold-eligible
/// early, and only executes once its actual due time arrives. The plan is built up front as a
/// List&lt;JobsQty&gt; and that same plan drives every assertion afterward, against both the Redis
/// tracker and the JobMaster API -- filtered by cluster alone, by cluster+handler, by
/// cluster+priority, and by cluster+TestIdentifier (per individual scheduled batch), proving each
/// filter dimension works both alone and combined with ClusterId.
///
/// A concrete scenario only needs to supply its (already kebab-cased) <see cref="ClusterIds"/> and
/// implement <see cref="BasePhaseEmulator{TPhaseEnum}.Phase"/> -- everything else is identical
/// regardless of which repo type is under test.
/// </summary>
public abstract class ScheduleTestPhase1EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private static readonly TimeSpan DelayAfter = TimeSpan.FromMinutes(5);

    // Mirrors JobMaster.Abstractions.Models.JobMasterJobStatus.Succeeded -- this project takes no
    // JobMaster reference (see ApiJob's Status being a raw JsonElement), so the value is inlined.
    private const int SucceededStatus = 5;

    // "verylong" (3min/job) is deliberately excluded from the plan: the standalone cluster only has
    // one worker, and even a couple of those jobs would eat into the budget this test relies on to
    // stay well clear of the 5-minute delay window.
    private static readonly Dictionary<string, string> HandlerTypeToJobDefinitionId = new()
    {
        ["fast"] = "TestApp.Fast",
        ["normal"] = "TestApp.Normal",
        ["slow"] = "TestApp.Slow",
        // Scheduled via IJobMasterScheduler.Advanced.OnceNow/OnceAfter<TDefinition>() instead of the
        // classic handler-typed overload -- proves the JobDefinitionConfigAttribute path end-to-end
        // (scheduling *and* execution-time handler resolution), not just in-process/mocked.
        ["advanced-fast"] = "TestApp.AdvancedFast"
    };

    // Raw JobMasterPriority underlying int values (VeryLow=1 .. Critical=5) -- gives the plan 4
    // distinct priorities to filter by, one per handler type.
    private static readonly Dictionary<string, int> HandlerTypeToPriority = new()
    {
        ["fast"] = 3,          // Medium
        ["normal"] = 4,        // High
        ["slow"] = 2,          // Low
        ["advanced-fast"] = 5  // Critical
    };

    protected abstract IReadOnlyList<string> ClusterIds { get; }

    public override async Task RunAsync()
    {
        var plan = BuildPlan(ClusterIds);

        // Schedule every batch up front so each delayed batch's 5-minute clock starts as early as
        // possible -- everything else below (immediate execution, API assertions) has to fit inside
        // that same window.
        var batches = new List<ScheduledBatch>();
        foreach (var qty in plan)
        {
            var testIdentifier = Guid.NewGuid().ToString("N");
            var batchScheduledAtUtc = DateTime.UtcNow;

            var scheduled = await Runner.ScheduleFor(qty.ClusterId)
                .ScheduleAsync(qty.HandlerType, testIdentifier, qtyJobs: qty.QtyJobs, clusterId: qty.ClusterId, afterSeconds: qty.AfterSecs, priority: qty.Priority);
            scheduled.JobIds.Should().HaveCount(qty.QtyJobs);

            batches.Add(new ScheduledBatch(qty, testIdentifier, scheduled.JobIds, batchScheduledAtUtc));
        }

        var immediateBatches = batches.Where(b => b.Plan.AfterSecs is null).ToList();
        var delayedBatches = batches.Where(b => b.Plan.AfterSecs is not null).ToList();

        // It cannot have executed right away: check immediately, before anything else runs.
        foreach (var batch in delayedBatches)
        {
            var executedSoFar = await Runner.Tracker.GetAllAsync(batch.TestIdentifier);
            executedSoFar.Should().BeEmpty(
                $"delayed batch {batch.Plan.ClusterId}/{batch.Plan.HandlerType} must not execute before its {batch.Plan.AfterSecs}s delay elapses");
        }

        // Immediate jobs should all complete quickly, well within the 5-minute delay window.
        foreach (var batch in immediateBatches)
        {
            var executions = await Runner.Tracker.WaitForAsync(batch.TestIdentifier, batch.Plan.QtyJobs, TimeSpan.FromMinutes(3));
            executions.Select(e => e.JobId).Should().BeEquivalentTo(batch.JobIds);
        }

        // Re-check after all that immediate work: still must not have executed early.
        foreach (var batch in delayedBatches)
        {
            var elapsed = DateTime.UtcNow - batch.ScheduledAtUtc;
            if (elapsed < DelayAfter)
            {
                var executedSoFar = await Runner.Tracker.GetAllAsync(batch.TestIdentifier);
                executedSoFar.Should().BeEmpty(
                    $"delayed batch {batch.Plan.ClusterId}/{batch.Plan.HandlerType} executed early, " +
                    $"{elapsed.TotalSeconds:F0}s after scheduling but before its {batch.Plan.AfterSecs}s delay");
            }
        }

        // Wait out whatever's left of each batch's delay, then confirm it executes -- and not before its due time.
        foreach (var batch in delayedBatches)
        {
            var dueAtUtc = batch.ScheduledAtUtc.AddSeconds(batch.Plan.AfterSecs!.Value);
            var remaining = dueAtUtc - DateTime.UtcNow;
            var timeout = (remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero) + TimeSpan.FromMinutes(2);

            var executions = await Runner.Tracker.WaitForAsync(batch.TestIdentifier, batch.Plan.QtyJobs, timeout);
            executions.Select(e => e.JobId).Should().BeEquivalentTo(batch.JobIds);

            var earliestExecutionUtc = executions.Min(e => e.ExecutedAtUtc);
            earliestExecutionUtc.Should().BeOnOrAfter(dueAtUtc.AddSeconds(-10),
                "a delayed job must not run meaningfully before its due time");
        }

        // Settle window + duplicate check across every batch, delayed and immediate alike.
        await Task.Delay(TimeSpan.FromSeconds(3));
        foreach (var batch in batches)
        {
            var all = await Runner.Tracker.GetAllAsync(batch.TestIdentifier);
            all.Should().HaveCount(batch.Plan.QtyJobs);
            all.Select(e => e.JobId).Should().OnlyHaveUniqueItems();
        }

        await AssertApiAsync(ClusterIds, batches);
    }

    private static List<JobsQty> BuildPlan(IReadOnlyList<string> clusterIds)
    {
        var delaySeconds = (int)DelayAfter.TotalSeconds;
        var plan = new List<JobsQty>();

        // Base quantities (150/50/3/100/30 = 333/cluster) are calibrated for the original 3-cluster
        // *Pure scenarios (999 total). Scaling by 3/clusterCount keeps every scenario's total near
        // ~1000 jobs regardless of how many clusters it has -- e.g. a 2-cluster scenario (no
        // standalone leg) scales up to 500/cluster (1000 total) instead of silently totaling 666.
        var scale = 3.0 / clusterIds.Count;

        foreach (var clusterId in clusterIds)
        {
            // Immediate.
            plan.Add(CreateQty(clusterId, "fast", Scale(150, scale)));
            plan.Add(CreateQty(clusterId, "normal", Scale(50, scale)));
            plan.Add(CreateQty(clusterId, "slow", Scale(3, scale)));

            plan.Add(CreateQty(clusterId, "advanced-fast", Scale(20, scale)));

            // Delayed by 5 minutes.
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

    private async Task AssertApiAsync(IReadOnlyList<string> clusterIds, IReadOnlyList<ScheduledBatch> batches)
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        foreach (var clusterId in clusterIds)
        {
            var clusterBatches = batches.Where(b => b.Plan.ClusterId == clusterId).ToList();

            // Filter: ClusterId only.
            var expectedClusterJobIds = clusterBatches.SelectMany(b => b.JobIds).ToHashSet();
            await AssertJobsAsync(api, clusterId, expectedClusterJobIds);

            // Filter: ClusterId + JobDefinitionId (one handler type at a time).
            foreach (var handlerType in clusterBatches.Select(b => b.Plan.HandlerType).Distinct())
            {
                var jobDefinitionId = HandlerTypeToJobDefinitionId[handlerType];
                var expectedHandlerJobIds = clusterBatches
                    .Where(b => b.Plan.HandlerType == handlerType)
                    .SelectMany(b => b.JobIds)
                    .ToHashSet();

                var handlerJobs = await AssertJobsAsync(api, clusterId, expectedHandlerJobIds, jobDefinitionId: jobDefinitionId);
                handlerJobs.Should().OnlyContain(j => j.JobDefinitionId == jobDefinitionId);
            }

            // Filter: ClusterId + Priority (one priority at a time).
            foreach (var priority in clusterBatches.Select(b => b.Plan.Priority).Distinct())
            {
                var expectedPriorityJobIds = clusterBatches
                    .Where(b => b.Plan.Priority == priority)
                    .SelectMany(b => b.JobIds)
                    .ToHashSet();

                var priorityJobs = await AssertJobsAsync(api, clusterId, expectedPriorityJobIds, priority: priority);
                priorityJobs.Should().OnlyContain(j => j.Priority.GetInt32() == priority);
            }

            // Filter: ClusterId + TestIdentifier only, no JobDefinitionId/Priority -- one scheduled
            // batch at a time. Proves metadata-based filtering works and each batch's exact job set
            // is independently visible via the API, not just aggregated per-cluster/per-handler.
            foreach (var batch in clusterBatches)
            {
                await AssertJobsAsync(api, clusterId, batch.JobIds.ToHashSet(), testIdentifier: batch.TestIdentifier);
            }
        }
    }

    private static async Task<List<ApiJob>> AssertJobsAsync(
        IScenarioApiClient api,
        string clusterId,
        IReadOnlySet<Guid> expectedJobIds,
        string? jobDefinitionId = null,
        int? priority = null,
        string? testIdentifier = null)
    {
        var count = await api.GetJobCountAsync(clusterId, jobDefinitionId, priority, testIdentifier);
        count.Should().Be(expectedJobIds.Count);

        var jobs = await api.GetJobsAsync(clusterId, jobDefinitionId, priority, testIdentifier, countLimit: int.MaxValue);
        jobs.Should().HaveCount(expectedJobIds.Count);
        jobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(expectedJobIds);
        jobs.Should().OnlyContain(j => j.Status.GetInt32() == SucceededStatus, "every job in this scenario must finish successfully");

        return jobs;
    }

    private sealed record ScheduledBatch(JobsQty Plan, string TestIdentifier, List<Guid> JobIds, DateTime ScheduledAtUtc);
}
