using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.RecurringScheduleTest;

/// <summary>
/// Shared phase-1 logic for every recurring-schedule scenario: exercises all 4 combinations of
/// (static vs dynamic registration) x (TimeSpanInterval vs NaturalCron compiler) against a single
/// standalone cluster, using a real Docker-container app (<c>TargetTestRecurringApp</c>), not an
/// in-process handler like <c>Tests/JobMaster.IntegrationTests</c>'s equivalent test.
///
/// Interval is a fixed 6 minutes for every combination -- deliberately in the middle of the
/// required (5min, 15min) exclusive range: long enough to avoid seconds-level granularity, short
/// enough to keep the observation window (~17 min) reasonable.
///
/// <b>TransientThreshold matters here</b>: RecurringSchedulePlanner floors its planning horizon at
/// max(TransientThreshold, 5 minutes) (JobMasterConstants.DurationToLockRecords, not configurable).
/// A 6-minute interval with the usual 2-minute TransientThreshold (floored to 5) would mean every
/// candidate occurrence always exceeds the horizon -- the schedule would stall forever, never firing
/// a single occurrence. The cluster config for this scenario MUST set TransientThreshold to
/// something above 6 minutes (this base uses 10 minutes) -- do not copy the 2-minute value from the
/// one-off ScheduleTest scenarios here.
///
/// <b>Compiler behavior is the same for both</b>: both TimeSpanInterval's and NaturalCron's
/// GetNextOccurrence are relative to whatever cursor they're given (cursor + interval) -- NaturalCron
/// is not wall-clock-grid-aligned (confirmed empirically: a schedule created at 17:16:21 fires its
/// first occurrence at 17:22:21, i.e. CreatedAt+6min, not the next :18:00 grid mark). First fire is
/// CreatedAt+Interval for both compilers; consecutive occurrences are exactly Interval apart for both
/// too. An earlier version of this test assumed NaturalCron aligned to a fixed :00/:06/:12/... grid --
/// that assumption was wrong and inflated the apparent first-firing delay by several minutes on every
/// NaturalCron run (Pure and NATS alike), since it compared actual (correct) CreatedAt+Interval timing
/// against a grid mark that was never the actual target.
/// </summary>
public abstract class RecurringScheduleTestPhase1EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    protected static readonly TimeSpan Interval = TimeSpan.FromMinutes(6);

    private const string TimeSpanIntervalExpressionTypeId = "TimeSpanInterval";
    private const string NaturalCronExpressionTypeId = "NaturalCron";
    private const string IntervalExpression = "00:06:00";
    private const string NaturalCronExpression = "every 6 minutes";

    // Must match the literal TestIdentifier strings baked into
    // Tests/TargetTestRecurringApp/StaticProfiles/*.cs -- different process/project, so these can't
    // be a shared constant; same duplication pattern already used for JobDefinitionId strings
    // between TargetTestScheduleApp and PureScheduleTestPhase1EmulatorBase.
    private const string StaticTimeSpanIntervalTestIdentifier = "static-timespan-interval";
    private const string StaticNaturalCronTestIdentifier = "static-natural-cron";

    private static readonly TimeSpan ImmediateCheckDelay = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan SettleWindowAfterCancel = TimeSpan.FromSeconds(90);

    /// <summary>
    /// How long to wait for each schedule to reach 2 executions. Still virtual in case a future
    /// provider genuinely needs more room, but no current variant (including NATS) does --
    /// RecurringSchedulePlanner always materializes/dispatches a schedule's next occurrence on its
    /// very first planning attempt now, regardless of whether that occurrence falls within the
    /// cluster's horizon, so there's no extra replanning cycle to budget for even when
    /// TransientThreshold is capped below the interval (NATS).
    /// </summary>
    protected virtual TimeSpan WaitForTwoFiringsTimeout => TimeSpan.FromMinutes(17);

    // How far off "first execution should land at CreatedAt+Interval" is tolerated, in either
    // direction -- covers container-startup/materialization latency and normal dispatch overhead
    // (including NATS publish/consumer-pickup), without being so wide it'd miss a genuine "fired far
    // too early or never fired near the right time" bug. Real measured delays are much smaller than
    // this (low tens of seconds at most across every DB/transport variant, including NATS, now that
    // RecurringSchedulePlanner always dispatches on the first attempt) -- 1 minute leaves comfortable
    // headroom without masking a real regression.
    private static readonly TimeSpan FirstFiringEarlyTolerance = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Same reasoning as <see cref="FirstFiringEarlyTolerance"/>. Still virtual for any future
    /// provider that turns out to need more room, but no current variant does.
    /// </summary>
    protected virtual TimeSpan FirstFiringLateTolerance => TimeSpan.FromMinutes(1);

    // Consecutive-firing spacing must average within this fraction of Interval -- mirrors
    // JobMasterSchedulerTests.RunRecurringScheduleTest's ±10% pattern.
    private const double SpacingTolerance = 0.10;

    protected abstract string ClusterId { get; }

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");
        var recurring = Runner.RecurringScheduleFor(ClusterId);

        // Captured right after the container's health check passed (StartPhaseAsync already
        // returned by the time RunAsync runs) -- a reasonably tight upper bound on when the static
        // profiles were actually registered, since /health only starts responding after
        // StartJobMasterRuntimeAsync (which bootstraps static profiles) completes.
        var staticCreatedAtUtc = DateTime.UtcNow;

        var dynamicTimeSpanId = Guid.NewGuid().ToString("N");
        var dynamicTimeSpan = await recurring.CreateRecurringAsync(
            "tick", TimeSpanIntervalExpressionTypeId, IntervalExpression, dynamicTimeSpanId, ClusterId);
        var dynamicTimeSpanCreatedAtUtc = DateTime.UtcNow;

        var dynamicNaturalCronId = Guid.NewGuid().ToString("N");
        var dynamicNaturalCron = await recurring.CreateRecurringAsync(
            "tick", NaturalCronExpressionTypeId, NaturalCronExpression, dynamicNaturalCronId, ClusterId);
        var dynamicNaturalCronCreatedAtUtc = DateTime.UtcNow;

        var combos = new[]
        {
            new Combo(StaticTimeSpanIntervalTestIdentifier, staticCreatedAtUtc),
            new Combo(StaticNaturalCronTestIdentifier, staticCreatedAtUtc),
            new Combo(dynamicTimeSpanId, dynamicTimeSpanCreatedAtUtc),
            new Combo(dynamicNaturalCronId, dynamicNaturalCronCreatedAtUtc),
        };

        // Nothing should have fired yet -- catches an accidental "fires immediately" bug (e.g. the
        // wrong scheduler method) well before the real wait below.
        await Task.Delay(ImmediateCheckDelay);
        foreach (var combo in combos)
        {
            var executedSoFar = await Runner.Tracker.GetAllAsync(combo.TestIdentifier);
            executedSoFar.Should().BeEmpty(
                $"'{combo.TestIdentifier}' must not fire within {ImmediateCheckDelay.TotalSeconds}s of being created/started");
        }

        // Wait for >=2 firings on all 4 concurrently -- sequential waits would blow the time budget
        // 4x over for no reason, since all 4 schedules tick independently.
        var waitTasks = combos.ToDictionary(
            c => c.TestIdentifier,
            c => Runner.Tracker.WaitForAsync(c.TestIdentifier, expectedCount: 2, WaitForTwoFiringsTimeout));
        await Task.WhenAll(waitTasks.Values);

        foreach (var combo in combos)
        {
            var executions = (await waitTasks[combo.TestIdentifier]).OrderBy(e => e.ExecutedAtUtc).ToList();
            executions.Should().HaveCountGreaterThanOrEqualTo(2, $"'{combo.TestIdentifier}' should have fired at least twice");
            executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems($"'{combo.TestIdentifier}' must not duplicate-execute a job");

            // First-firing timing: both compilers are relative to creation (CreatedAt + Interval) --
            // see the class remarks on why NaturalCron isn't wall-clock-grid-aligned despite reading
            // like a cron expression.
            var firstExecUtc = executions[0].ExecutedAtUtc;
            var expectedFirstUtc = combo.CreatedAtUtc + Interval;

            (firstExecUtc - expectedFirstUtc).TotalSeconds.Should().BeInRange(
                -FirstFiringEarlyTolerance.TotalSeconds, FirstFiringLateTolerance.TotalSeconds,
                $"'{combo.TestIdentifier}' first execution should land close to its expected due time, not early or badly late");

            // Consecutive-firing spacing: shared check, both compilers fire exactly Interval apart
            // once started.
            var gaps = executions.Zip(executions.Skip(1), (a, b) => (b.ExecutedAtUtc - a.ExecutedAtUtc).TotalSeconds).ToList();
            var averageGapSeconds = gaps.Average();
            var toleranceSeconds = Interval.TotalSeconds * SpacingTolerance;
            averageGapSeconds.Should().BeInRange(
                Interval.TotalSeconds - toleranceSeconds, Interval.TotalSeconds + toleranceSeconds,
                $"'{combo.TestIdentifier}' consecutive executions should be spaced ~{Interval.TotalMinutes} minutes apart");
        }

        // Cross-check via the API -- proves the schedule is visible/correct there too, not just
        // that jobs happened to execute.
        await AssertApiAsync(api, ClusterId, dynamicTimeSpanId, dynamicTimeSpan.RecurringScheduleId, TimeSpanIntervalExpressionTypeId, IntervalExpression);
        await AssertApiAsync(api, ClusterId, dynamicNaturalCronId, dynamicNaturalCron.RecurringScheduleId, NaturalCronExpressionTypeId, NaturalCronExpression);

        // Cancel only the dynamic schedules -- static ones aren't cancellable the same way and are
        // simply left running until the container stops.
        var countAtCancelTimeSpan = (await Runner.Tracker.GetAllAsync(dynamicTimeSpanId)).Count;
        var countAtCancelNaturalCron = (await Runner.Tracker.GetAllAsync(dynamicNaturalCronId)).Count;
        await recurring.CancelRecurringAsync(dynamicTimeSpan.RecurringScheduleId, ClusterId);
        await recurring.CancelRecurringAsync(dynamicNaturalCron.RecurringScheduleId, ClusterId);

        // CancelJobsFromRecurScheduleInactiveOrCanceledRunner ticks every 30s; give it margin to
        // actually process the cancellation before checking nothing further fired.
        await Task.Delay(SettleWindowAfterCancel);

        var afterCancelTimeSpan = await Runner.Tracker.GetAllAsync(dynamicTimeSpanId);
        var afterCancelNaturalCron = await Runner.Tracker.GetAllAsync(dynamicNaturalCronId);

        // At most one more execution is tolerated: a job already in-flight at cancellation time
        // isn't retroactively killed, only future occurrences are prevented.
        afterCancelTimeSpan.Count.Should().BeLessThanOrEqualTo(countAtCancelTimeSpan + 1,
            "cancelling the dynamic TimeSpanInterval schedule should stop further firings (at most one in-flight execution tolerated)");
        afterCancelNaturalCron.Count.Should().BeLessThanOrEqualTo(countAtCancelNaturalCron + 1,
            "cancelling the dynamic NaturalCron schedule should stop further firings (at most one in-flight execution tolerated)");
    }

    private static async Task AssertApiAsync(IScenarioApiClient api, string clusterId, string testIdentifier, Guid recurringScheduleId, string expectedExpressionTypeId, string expectedExpression)
    {
        var schedules = await api.GetRecurringSchedulesAsync(clusterId, testIdentifier: testIdentifier);
        schedules.Should().ContainSingle(s => GuidBase64.Parse(s.Id) == recurringScheduleId)
            .Which.Should().Match<ApiRecurringSchedule>(s =>
                s.ExpressionTypeId == expectedExpressionTypeId &&
                s.Expression == expectedExpression &&
                s.JobDefinitionId == "RecurringApp.Tick");
    }

    private sealed record Combo(string TestIdentifier, DateTime CreatedAtUtc);
}
