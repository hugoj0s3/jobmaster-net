using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioTests.Scenarios.StandaloneToDistributedTest;

/// <summary>
/// Shared phase-2 logic for every repo-type variant of this scenario: a genuinely distributed
/// topology (separate Coordinator + Execution containers, on a brand new named agent connection --
/// never the reserved standalone one) takes over the same ClusterId and database Phase1's now-dead
/// standalone worker owned. Proves every job Phase1 scheduled actually finishes here, none lost,
/// none duplicated.
///
/// Recovery goes through two layers, both real unmodified JobMaster runners:
/// - Fast path: JobMasterRuntime detects buckets still tagged as standalone-owned when this
///   non-standalone config starts, and auto-synthesizes a "StandaloneDrainer" (Drain-mode) worker
///   on the reserved standalone connection -- the same connection string as master, so it's a live
///   worker on the exact same AgentConnectionId the dead standalone worker's buckets are pinned
///   to. MarkBucketAsLostRunner (2.5 min tick) marks the orphaned bucket Lost, then
///   AssignedLostBucketsRunner (1 min tick) finds the auto-synthesized drainer alive on that same
///   connection and marks it ReadyToDrain -- ordinary drain machinery from there.
/// - Slow backstop: any job still stuck in-bucket if the fast path somehow missed it gets reclaimed
///   by HeldOnMasterDeadlineTimeoutJobsRunner once its ProcessDeadline (10 min from assignment)
///   elapses while its bucket isn't Active/Completing -- a Lost bucket qualifies.
/// Either way, once a job is back to OnMaster, AssignJobsToBucketsRunner (10s probe) picks it up
/// and assigns it to the new cluster's real bucket on the new agent connection.
///
/// A concrete scenario only needs to supply its (already kebab-cased) <see cref="ClusterId"/> and
/// its Phase2 agent connection's <see cref="AgentConnectionName"/>, implement <see cref="LoadState"/>
/// to read back the test identifier + scheduled job IDs Phase1 stored, and implement
/// <see cref="BasePhaseEmulator{TPhaseEnum}.Phase"/>.
/// </summary>
public abstract class StandaloneToDistributedTestPhase2EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private const int QtyJobs = 300;
    private const int SucceededStatus = 5;

    // Budgeted past the ~10-minute worst-case ProcessDeadline backstop plus onboarding/execution
    // latency, even though the fast auto-drain path should dominate in practice.
    private static readonly TimeSpan FinalizeTimeout = TimeSpan.FromMinutes(20);

    protected abstract string ClusterId { get; }
    protected abstract string AgentConnectionName { get; }

    protected abstract (string TestIdentifier, List<Guid> JobIds) LoadState();

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        var (testIdentifier, scheduledJobIds) = LoadState();
        scheduledJobIds.Should().HaveCount(QtyJobs, "Phase1 must have captured its scheduled job IDs");

        var executions = await Runner.Tracker.WaitForAsync(testIdentifier, QtyJobs, FinalizeTimeout);
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        var apiJobs = await api.GetJobsAsync(ClusterId, testIdentifier: testIdentifier, status: SucceededStatus, countLimit: int.MaxValue);
        apiJobs.Should().HaveCount(QtyJobs);
        apiJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(scheduledJobIds,
            "recovery must preserve the exact same job IDs -- no job lost, none executed twice, across the standalone-to-distributed transition");

        (await api.GetJobCountAsync(ClusterId, testIdentifier: testIdentifier, status: SucceededStatus)).Should().Be(QtyJobs);
        (await api.GetJobCountAsync(ClusterId, testIdentifier: testIdentifier)).Should().Be(QtyJobs);

        var connections = (await api.GetAgentConnectionsAsync(ClusterId)).ExcludingReserved().ToList();
        connections.Should().ContainSingle(c => c.Name == AgentConnectionName && c.IsAlive);
    }
}
