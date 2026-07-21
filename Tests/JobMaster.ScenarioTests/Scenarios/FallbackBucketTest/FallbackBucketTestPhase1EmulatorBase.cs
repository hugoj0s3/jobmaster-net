using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.FallbackBucketTest;

/// <summary>
/// Shared phase-1 logic for every repo-type variant of this scenario (PostgresPure, MySqlPure,
/// SqlServerPure, PostgresNats): a Coordinator-only cluster -- no AgentConnections at all -- proves
/// AssignJobsToBucketsRunner's fallback-bucket mechanism actually keeps jobs moving instead of
/// starving them forever. Once no real bucket can be found for a job for over
/// JobMasterConstants.NoBucketFallbackThreshold (2.5 minutes), the Coordinator creates a
/// BucketType.Fallback bucket backed by a reserved, master-DB-backed connection and services it
/// with an inline PollingJobsExecutionRunner on its own process -- no real agent worker ever
/// needed.
///
/// A concrete scenario only needs to supply its (already kebab-cased) <see cref="ClusterId"/> and
/// <see cref="ContainerName"/>, and implement <see cref="BasePhaseEmulator{TPhaseEnum}.Phase"/> --
/// everything else is identical regardless of which repo type is under test.
/// </summary>
public abstract class FallbackBucketTestPhase1EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private const int QtyJobs = 30;
    private const int SucceededStatus = 5;
    private const int FallbackBucketType = 2;

    // NoBucketFallbackThreshold is a fixed 2.5-minute framework constant (not configurable per
    // cluster) -- budget generously past it plus onboarding/execution latency.
    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(10);

    protected abstract string ClusterId { get; }
    protected abstract string ContainerName { get; }

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        // Generated per run, not a literal constant: every repo-type variant of this scenario
        // shares one Redis instance (ScenarioGlobalEnvironment is a single run-scoped fixture), and
        // xUnit's ScenarioCollection runs them sequentially rather than concurrently -- but Redis
        // data from an earlier variant's run isn't cleared afterward. A literal TestIdentifier
        // would let a later variant's Tracker.WaitForAsync find an earlier variant's already-
        // recorded executions and return immediately, well before this run's own jobs finish.
        var testIdentifier = Guid.NewGuid().ToString("N");

        var scheduled = await Runner.ScheduleFor(ContainerName)
            .ScheduleAsync("fast", testIdentifier, QtyJobs, clusterId: ClusterId);
        scheduled.JobIds.Should().HaveCount(QtyJobs);

        var executions = await Runner.Tracker.WaitForAsync(testIdentifier, QtyJobs, ExecuteWaitTimeout);
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        var apiJobs = await api.GetJobsAsync(ClusterId, testIdentifier: testIdentifier, status: SucceededStatus, countLimit: int.MaxValue);
        apiJobs.Should().HaveCount(QtyJobs);
        apiJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(scheduled.JobIds,
            "the fallback bucket dispatches jobs directly, it never re-creates them with a new Id");

        var buckets = await api.GetBucketsAsync(ClusterId, countLimit: int.MaxValue);
        buckets.Should().ContainSingle(b => b.BucketType.GetInt32() == FallbackBucketType,
            "no real bucket could ever exist on a Coordinator-only cluster -- every job must have gone through the one fallback bucket");
    }
}
