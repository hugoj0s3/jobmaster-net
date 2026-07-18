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
/// enough to keep the observation window (~17 min) reasonable, and it divides 60 cleanly so the
/// NaturalCron schedule's wall-clock-aligned firing pattern (":00/:06/:12/...") never straddles an
/// hour boundary awkwardly during the wait.
///
/// <b>TransientThreshold matters here</b>: RecurringSchedulePlanner floors its planning horizon at
/// max(TransientThreshold, 5 minutes) (JobMasterConstants.DurationToLockRecords, not configurable).
/// A 6-minute interval with the usual 2-minute TransientThreshold (floored to 5) would mean every
/// candidate occurrence always exceeds the horizon -- the schedule would stall forever, never firing
/// a single occurrence. The cluster config for this scenario MUST set TransientThreshold to
/// something above 6 minutes (this base uses 10 minutes) -- do not copy the 2-minute value from the
/// one-off ScheduleTest scenarios here.
///
/// <b>Compiler behavioral difference</b>: TimeSpanInterval's GetNextOccurrence is relative
/// (cursor.Add(interval) -- first fire is exactly `interval` after creation). NaturalCron
/// "every 6 minutes" is wall-clock-aligned (cron-style, fires at fixed :00/:06/:12/... marks) --
/// first-fire delay after creation is variable, not a fixed 6 minutes. Once firing has started,
/// consecutive occurrences ARE exactly 6 minutes apart for both compilers (that part of the
/// assertion is shared); only the first-occurrence timing check differs.
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
    /// How long to wait for each schedule to reach 2 executions. Virtual so a provider whose
    /// planning horizon is capped below the interval (NATS -- see
    /// <c>RecurringScheduleTest.PostgresNats</c>'s override) can budget for the extra replanning
    /// cycle its first occurrence may need.
    /// </summary>
    protected virtual TimeSpan WaitForTwoFiringsTimeout => TimeSpan.FromMinutes(17);

    // How far off "first execution should land at CreatedAt+Interval" is tolerated, in either
    // direction -- wide enough to absorb container-startup/materialization latency (especially for
    // the static schedules, whose true creation time is a little before this phase's own
    // DateTime.UtcNow checkpoint) without being so wide it'd miss a genuine "fired far too early or
    // never fired near the right time" bug. Observed in practice: a static NaturalCron schedule's
    // first execution landed ~208s after the naive CreatedAt+Interval-style estimate (the gap
    // between "container health check passed" and the schedule's true registration moment, plus
    // one materialization cycle, adds up) -- 5 minutes covers that with headroom.
    private static readonly TimeSpan FirstFiringEarlyTolerance = TimeSpan.FromMinutes(1.5);

    /// <summary>
    /// Virtual for the same reason as <see cref="WaitForTwoFiringsTimeout"/>: a capped planning
    /// horizon (NATS) can genuinely delay the first occurrence's materialization by up to one extra
    /// replanning cycle beyond the naive CreatedAt+Interval estimate.
    /// </summary>
    protected virtual TimeSpan FirstFiringLateTolerance => TimeSpan.FromMinutes(5);

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
            new Combo(StaticTimeSpanIntervalTestIdentifier, IsNaturalCron: false, staticCreatedAtUtc),
            new Combo(StaticNaturalCronTestIdentifier, IsNaturalCron: true, staticCreatedAtUtc),
            new Combo(dynamicTimeSpanId, IsNaturalCron: false, dynamicTimeSpanCreatedAtUtc),
            new Combo(dynamicNaturalCronId, IsNaturalCron: true, dynamicNaturalCronCreatedAtUtc),
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

            // First-firing timing: TimeSpanInterval is relative to creation; NaturalCron is
            // wall-clock-aligned, so its "expected" first-fire time is the next 6-minute mark from
            // creation, not a fixed offset.
            var firstExecUtc = executions[0].ExecutedAtUtc;
            var expectedFirstUtc = combo.IsNaturalCron
                ? NextSixMinuteMarkAtOrAfter(combo.CreatedAtUtc)
                : combo.CreatedAtUtc + Interval;

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

    private static DateTime NextSixMinuteMarkAtOrAfter(DateTime utc)
    {
        var flooredMinute = utc.Minute - (utc.Minute % 6);
        var floored = new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, flooredMinute, 0, DateTimeKind.Utc);
        return floored <= utc ? floored.AddMinutes(6) : floored;
    }

    private sealed record Combo(string TestIdentifier, bool IsNaturalCron, DateTime CreatedAtUtc);
}
