using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ScheduleTest;

/// <summary>
/// Shared phase-1 logic for every "Pure" single-repo-type schedule test (PostgresPure, MySqlPure,
/// SqlServerPure, ...): schedules ~1000 jobs across all of the scenario's clusters, mixing
/// immediate and 5-minute-delayed jobs across several handler types. Every cluster's
/// TransientThreshold is 2 minutes (see each scenario's Phase1/*.json) -- well under the 5-minute
/// delay -- so this also proves a delayed job is not dispatched the moment it's threshold-eligible
/// early, and only executes once its actual due time arrives. The plan is built up front as a
/// List&lt;JobsQty&gt; and that same plan drives every assertion afterward, against both the Redis
/// tracker and the JobMaster API.
///
/// A concrete scenario only needs to supply its (already kebab-cased) <see cref="ClusterIds"/> and
/// implement <see cref="BasePhaseEmulator{TPhaseEnum}.Phase"/> -- everything else is identical
/// regardless of which repo type is under test.
/// </summary>
public abstract class PureScheduleTestPhase1EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private static readonly TimeSpan DelayAfter = TimeSpan.FromMinutes(5);

    // "verylong" (3min/job) is deliberately excluded from the plan: the standalone cluster only has
    // one worker, and even a couple of those jobs would eat into the budget this test relies on to
    // stay well clear of the 5-minute delay window.
    private static readonly Dictionary<string, string> HandlerTypeToJobDefinitionId = new()
    {
        ["fast"] = "TestApp.Fast",
        ["normal"] = "TestApp.Normal",
        ["slow"] = "TestApp.Slow"
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
                .ScheduleAsync(qty.HandlerType, testIdentifier, qtyJobs: qty.QtyJobs, clusterId: qty.ClusterId, afterSeconds: qty.AfterSecs);
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

        foreach (var clusterId in clusterIds)
        {
            // Immediate: 150 + 50 + 3 = 203/cluster.
            plan.Add(new JobsQty { ClusterId = clusterId, HandlerType = "fast", QtyJobs = 150 });
            plan.Add(new JobsQty { ClusterId = clusterId, HandlerType = "normal", QtyJobs = 50 });
            plan.Add(new JobsQty { ClusterId = clusterId, HandlerType = "slow", QtyJobs = 3 });

            // Delayed by 5 minutes: 100 + 30 = 130/cluster.
            plan.Add(new JobsQty { ClusterId = clusterId, HandlerType = "fast", QtyJobs = 100, AfterSecs = delaySeconds });
            plan.Add(new JobsQty { ClusterId = clusterId, HandlerType = "normal", QtyJobs = 30, AfterSecs = delaySeconds });
        }

        // 333/cluster x N clusters -- effectively the ~1000 jobs asked for (999 at N=3).
        return plan;
    }

    private async Task AssertApiAsync(IReadOnlyList<string> clusterIds, IReadOnlyList<ScheduledBatch> batches)
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        foreach (var clusterId in clusterIds)
        {
            var clusterBatches = batches.Where(b => b.Plan.ClusterId == clusterId).ToList();
            var expectedClusterJobIds = clusterBatches.SelectMany(b => b.JobIds).ToHashSet();

            var clusterCount = await api.GetJobCountAsync(clusterId);
            clusterCount.Should().Be(expectedClusterJobIds.Count);

            var clusterJobs = await api.GetJobsAsync(clusterId, countLimit: int.MaxValue);
            clusterJobs.Should().HaveCount(expectedClusterJobIds.Count);
            clusterJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(expectedClusterJobIds);

            foreach (var handlerType in clusterBatches.Select(b => b.Plan.HandlerType).Distinct())
            {
                var jobDefinitionId = HandlerTypeToJobDefinitionId[handlerType];
                var expectedHandlerJobIds = clusterBatches
                    .Where(b => b.Plan.HandlerType == handlerType)
                    .SelectMany(b => b.JobIds)
                    .ToHashSet();

                var handlerCount = await api.GetJobCountAsync(clusterId, jobDefinitionId);
                handlerCount.Should().Be(expectedHandlerJobIds.Count);

                var handlerJobs = await api.GetJobsAsync(clusterId, jobDefinitionId, countLimit: int.MaxValue);
                handlerJobs.Should().HaveCount(expectedHandlerJobIds.Count);
                handlerJobs.Should().OnlyContain(j => j.JobDefinitionId == jobDefinitionId);
                handlerJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(expectedHandlerJobIds);
            }
        }
    }

    private sealed record ScheduledBatch(JobsQty Plan, string TestIdentifier, List<Guid> JobIds, DateTime ScheduledAtUtc);
}
