using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioTests.Scenarios.ArchivedModeTest;

/// <summary>
/// Shared phase-1 logic for every repo-type variant of this scenario (PostgresPure, MySqlPure,
/// SqlServerPure, PostgresNats, RavenDbPure): one container hosts two clusters (DeleteOldFinalJobsRunner
/// cross-cluster forwards via <c>JobMasterClusterAwareComponentFactories.TryGetFactory</c>, which
/// only resolves clusters registered in the same process) -- an entirely ordinary Active source
/// (but with DataRetentionTtl set to JobMasterDefaults.MinDataRetentionTtl -- 10 minutes, the
/// framework's floor -- and TargetArchivedClusterId set) and an Archived target (no agent
/// connections, only a Coordinator, per PreValidation's "Archive clusters only run Coordinator").
/// Proves the real, unmodified archive-then-delete flow (DeleteOldFinalJobsRunner's adaptive poll
/// -- Clamp(TTL/2, 5min, 1hr) => 5 minutes at this TTL -- archiving via the same
/// BulkInsertIfNotExistsAsync/intake-service path RepoConformance covers, then deleting the source
/// rows) actually moves finished jobs across intact: same job Id, same Succeeded status, none
/// lost, none duplicated. Also covers DeleteOldInactiveRecurringSchedulesRunner the same way,
/// gated by the same DataRetentionTtl and TargetArchivedClusterId: a terminated recurring schedule
/// is archived and purged the same way a finalized job is.
///
/// A concrete scenario only needs to supply its (already kebab-cased) <see cref="SourceClusterId"/>,
/// <see cref="TargetClusterId"/>, and <see cref="ContainerName"/>, and implement
/// <see cref="BasePhaseEmulator{TPhaseEnum}.Phase"/> -- everything else is identical regardless of
/// which repo type is under test.
/// </summary>
public abstract class ArchivedModeTestPhase1EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private const string TimeSpanIntervalExpressionTypeId = "TimeSpanInterval";
    private const string RecurringInterval = "00:06:00";
    private const int QtyJobs = 200;
    private const int SucceededStatus = 5;
    private const int FailedStatus = 7;
    private const int CanceledStatus = 3;
    // Unlike Status/Priority/Category (raw ints), ApiJobExecution.Outcome is serialized as the enum's
    // string name (ApiJobExecution.FromDto: Outcome = execution.Outcome.ToString()).
    private const string SucceededOutcome = "Succeeded";
    private const string FailedOutcome = "Failed";
    private const int JobExecutionLogCategory = 2;
    private const int ExecutionSampleSize = 5;

    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(10);

    // DataRetentionTtl is the framework's 10-minute floor; the purge runner's own poll interval is
    // derived from it (max(5min, min(ttl/2, 1hr)) => 5 minutes here). Budget generously past both the
    // TTL itself and at least one extra tick -- matches DataRetentionPhase2EmulatorBase's calibration.
    private static readonly TimeSpan ArchiveWaitTimeout = TimeSpan.FromMinutes(20);

    protected abstract string SourceClusterId { get; }
    protected abstract string TargetClusterId { get; }
    protected abstract string ContainerName { get; }

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        // Generated per run, not literal constants: every repo-type variant of this scenario shares
        // one Redis instance (ScenarioGlobalEnvironment is a single run-scoped fixture), and xUnit's
        // ScenarioCollection runs them sequentially rather than concurrently -- but Redis data from
        // an earlier variant's run isn't cleared afterward. A literal TestIdentifier would let a
        // later variant's Tracker.WaitForAsync find an earlier variant's already-recorded
        // executions and return immediately, well before this run's own jobs finish.
        var testIdentifier = Guid.NewGuid().ToString("N");
        var recurringTestIdentifier = Guid.NewGuid().ToString("N");

        // One job of the QtyJobs total deliberately fails its only attempt (InjectFailure,
        // maxNumberOfRetries: 0 -- so it's permanently Failed after one attempt, not stuck in a
        // multi-minute retry backoff), so this scenario also proves a Failed JobExecution row and its
        // JobExecution-category log (only ever written on a failed attempt -- see
        // JobsExecutionEngine.HandleErrorAsync) both survive archiving, not just the happy-path row.
        var scheduled = await Runner.ScheduleFor(ContainerName)
            .ScheduleAsync("fast", testIdentifier, QtyJobs - 1, clusterId: SourceClusterId);
        var failureInjected = await Runner.ScheduleFor(ContainerName)
            .ScheduleAsync("fast", testIdentifier, qtyJobs: 1, clusterId: SourceClusterId, injectFailure: true, maxNumberOfRetries: 0);
        var allJobIds = scheduled.JobIds.Concat(failureInjected.JobIds).ToList();
        allJobIds.Should().HaveCount(QtyJobs);
        var failedJobId = failureInjected.JobIds.Single();

        var recurring = await Runner.RecurringScheduleFor(ContainerName)
            .CreateRecurringAsync("fast", TimeSpanIntervalExpressionTypeId, RecurringInterval, recurringTestIdentifier, clusterId: SourceClusterId);

        // The source cluster is an ordinary Active cluster with a real bucket/agent worker, so
        // creating a recurring schedule here dispatches it (PendingSave) to the agent worker rather
        // than inserting it into jm_recurring_schedule synchronously -- unlike MigratingModeTest's
        // source, which has no buckets at all and always saves synchronously. Cancelling before the
        // dispatch settles 404s because the row isn't visible yet, so wait for it first.
        await PollingWaitUtil.WaitUntilAsync(TimeSpan.FromSeconds(30),
            async () => (await api.GetRecurringSchedulesAsync(SourceClusterId, recurringTestIdentifier)).Count == 1,
            "the newly created recurring schedule to be persisted (dispatched to its bucket) before cancelling it",
            pollInterval: TimeSpan.FromMilliseconds(500));

        await Runner.RecurringScheduleFor(ContainerName)
            .CancelRecurringAsync(recurring.RecurringScheduleId, clusterId: SourceClusterId);

        // The source cluster is an ordinary Active cluster -- jobs onboard and execute normally. Waited
        // for separately from the failure-injected job below: with maxNumberOfRetries: 0 that job never
        // reaches Succeeded (never calls recorder.RecordAsync), so it must not count toward QtyJobs here.
        var executions = await Runner.Tracker.WaitForAsync(testIdentifier, QtyJobs - 1, ExecuteWaitTimeout);
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        await PollingWaitUtil.WaitUntilAsync(ExecuteWaitTimeout,
            async () => (await api.GetJobAsync(SourceClusterId, failedJobId))?.Status.GetInt32() == FailedStatus,
            $"the failure-injected job {failedJobId} to exhaust its one allowed attempt and reach Failed",
            pollInterval: TimeSpan.FromMilliseconds(500));

        (await api.GetJobCountAsync(SourceClusterId, testIdentifier: testIdentifier, status: SucceededStatus)).Should().Be(QtyJobs - 1);
        (await api.GetJobCountAsync(SourceClusterId, testIdentifier: testIdentifier, status: FailedStatus)).Should().Be(1);

        // The real archive-then-delete lifecycle: once DataRetentionTtl elapses, DeleteOldFinalJobsRunner
        // archives these finalized jobs into the target cluster then deletes them from the source.
        await PollingWaitUtil.WaitUntilAsync(ArchiveWaitTimeout,
            async () => await api.GetJobCountAsync(SourceClusterId, testIdentifier: testIdentifier) == 0,
            "every finalized job on the Active cluster to be archived and purged after DataRetentionTtl",
            pollInterval: TimeSpan.FromSeconds(30));

        var archivedJobs = await api.GetJobsAsync(TargetClusterId, testIdentifier: testIdentifier, countLimit: int.MaxValue);
        archivedJobs.Should().HaveCount(QtyJobs);
        archivedJobs.Select(j => GuidBase64.Parse(j.Id)).Should().BeEquivalentTo(allJobIds,
            "archiving must preserve the exact same job IDs -- the intake service reassigns ClusterId only, it never re-creates the job with a new Id");
        archivedJobs.Where(j => GuidBase64.Parse(j.Id) != failedJobId).Should().OnlyContain(j => j.Status.GetInt32() == SucceededStatus,
            "the archived copy must keep the Succeeded status it had at purge time");
        archivedJobs.Single(j => GuidBase64.Parse(j.Id) == failedJobId).Status.GetInt32().Should().Be(FailedStatus,
            "the archived copy of the failure-injected job must keep the Failed status it had at purge time");

        // JobExecutions are recorded unconditionally for every attempt (unlike JobExecution-category
        // logs, which only appear on a failed attempt) -- check a bounded sample of ordinary jobs
        // archived their (single, Succeeded) execution row intact.
        foreach (var jobId in scheduled.JobIds.Take(ExecutionSampleSize))
        {
            var sampleExecutions = await api.GetJobExecutionsAsync(TargetClusterId, jobId);
            sampleExecutions.Should().ContainSingle(e => e.Outcome.GetString() == SucceededOutcome,
                $"job {jobId}'s single successful execution attempt must have been archived alongside it");
        }

        // The failure-injected job's single Failed execution row, and the JobExecution-category log its
        // one attempt wrote, must have survived archiving.
        var failedJobExecutions = await api.GetJobExecutionsAsync(TargetClusterId, failedJobId);
        failedJobExecutions.Should().ContainSingle(e => e.Outcome.GetString() == FailedOutcome,
            "the failure-injected job's Failed execution must have been archived alongside it");

        var failedJobLogs = await api.GetLogsAsync(TargetClusterId, failedJobId, category: JobExecutionLogCategory);
        failedJobLogs.Should().NotBeEmpty(
            "the failed attempt's JobExecution-category log must have been archived alongside the job, " +
            "not lost the way DeleteOldLogsRunner's blanket purge would otherwise have raced it away");

        // Same DataRetentionTtl/TargetArchivedClusterId drive DeleteOldInactiveRecurringSchedulesRunner
        // too -- the cancelled schedule above is a terminated-recurring-schedule candidate from the
        // moment it was cancelled, same as the jobs above are candidates from the moment they succeeded.
        await PollingWaitUtil.WaitUntilAsync(ArchiveWaitTimeout,
            async () => (await api.GetRecurringSchedulesAsync(SourceClusterId, recurringTestIdentifier)).Count == 0,
            "the terminated recurring schedule on the Active cluster to be archived and purged after DataRetentionTtl",
            pollInterval: TimeSpan.FromSeconds(30));

        var archivedSchedules = await api.GetRecurringSchedulesAsync(TargetClusterId, recurringTestIdentifier);
        archivedSchedules.Should().ContainSingle();
        var archivedSchedule = archivedSchedules[0];
        GuidBase64.Parse(archivedSchedule.Id).Should().Be(recurring.RecurringScheduleId);
        archivedSchedule.Status.GetInt32().Should().Be(CanceledStatus,
            "the archived copy must keep the Canceled status it had at purge time");
    }
}
