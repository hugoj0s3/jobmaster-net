using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioMatrixTests.Lib;

namespace JobMaster.ScenarioMatrixTests.Scenarios.RecurringScheduleTest;

/// <summary>
/// Recurring schedules fire on time, exactly once per occurrence, for 8 registration/compiler combinations: static
/// vs dynamic × TimeSpanInterval vs NaturalCron, attribute-only static registration, the Advanced
/// (<c>JobDefinitionConfigAttribute</c>) dynamic path, and the Cronos / NCrontab satellite compilers (static only).
/// Every combination fires every 6 minutes. See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class RecurringScheduleTests(ScenarioGlobalEnvironment global)
{
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(6);

    private const string TimeSpanIntervalExpressionTypeId = "TimeSpanInterval";
    private const string NaturalCronExpressionTypeId = "NaturalCron";
    private const string IntervalExpression = "00:06:00";
    private const string NaturalCronExpression = "every 6 minutes";

    // Must match the literal TestIdentifier strings in tests/TargetTestRecurringApp (StaticProfiles/*.cs and
    // Handlers/AttributeStaticTickHandler.cs): different process/project, so no shared constant.
    private const string StaticTimeSpanIntervalTestIdentifier = "static-timespan-interval";
    private const string StaticNaturalCronTestIdentifier = "static-natural-cron";
    private const string StaticCronosTestIdentifier = "static-cronos";
    private const string StaticNCrontabTestIdentifier = "static-ncrontab";
    private const string AttributeStaticTickTestIdentifier = "attribute-static-tick";

    private static readonly TimeSpan ImmediateCheckDelay = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan SettleWindowAfterCancel = TimeSpan.FromSeconds(90);
    private static readonly TimeSpan WaitForTwoFiringsTimeout = TimeSpan.FromMinutes(17);

    // First occurrence of a creation-relative schedule: CreatedAt + Interval, give or take startup.
    private static readonly TimeSpan FirstOccurrenceTolerance = TimeSpan.FromMinutes(1);

    // How long after its ScheduledAt a job may run (dispatch, NATS publish, pickup), and how early (clocks).
    private static readonly TimeSpan RunLateTolerance = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan RunEarlyTolerance = TimeSpan.FromSeconds(5);

    // Consecutive ScheduledAt values must be exactly one Interval apart (stored times can lose sub-second precision).
    private static readonly TimeSpan OccurrenceGapTolerance = TimeSpan.FromSeconds(1);

    public static IEnumerable<object[]> Flavors() =>
        ScenarioFlavors.AllExcept().Select(flavor => new object[] { flavor.Name });

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Run(string flavor)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "RecurringScheduleTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));
        await runner.StartPhaseAsync("phase-1");

        var clusterId = $"mx-{selected.Code}-recurring";
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");
        var recurring = runner.RecurringScheduleFor(clusterId);

        // The static identifiers are fixed literals in the app; clear what an earlier flavor recorded.
        await runner.Tracker.ClearAsync(StaticTimeSpanIntervalTestIdentifier);
        await runner.Tracker.ClearAsync(StaticNaturalCronTestIdentifier);
        await runner.Tracker.ClearAsync(AttributeStaticTickTestIdentifier);
        await runner.Tracker.ClearAsync(StaticCronosTestIdentifier);
        await runner.Tracker.ClearAsync(StaticNCrontabTestIdentifier);

        // Right after the health check passed: /health only answers once the runtime (which registers the static
        // profiles) has started, so this is a tight upper bound on when the static schedules were created.
        var staticCreatedAtUtc = DateTime.UtcNow;

        var dynamicTimeSpanId = Guid.NewGuid().ToString("N");
        var dynamicTimeSpan = await recurring.CreateRecurringAsync(
            "tick", TimeSpanIntervalExpressionTypeId, IntervalExpression, dynamicTimeSpanId, clusterId);
        var dynamicTimeSpanCreatedAtUtc = DateTime.UtcNow;

        var dynamicNaturalCronId = Guid.NewGuid().ToString("N");
        var dynamicNaturalCron = await recurring.CreateRecurringAsync(
            "tick", NaturalCronExpressionTypeId, NaturalCronExpression, dynamicNaturalCronId, clusterId);
        var dynamicNaturalCronCreatedAtUtc = DateTime.UtcNow;

        // Through IJobMasterScheduler.Advanced.RecurringAsync<TDefinition>() instead of the handler-typed overload.
        var dynamicAdvancedId = Guid.NewGuid().ToString("N");
        var dynamicAdvanced = await recurring.CreateRecurringAsync(
            "advanced-tick", TimeSpanIntervalExpressionTypeId, IntervalExpression, dynamicAdvancedId, clusterId);
        var dynamicAdvancedCreatedAtUtc = DateTime.UtcNow;

        // TimeSpanInterval and NaturalCron fire relative to creation (CreatedAt + n × Interval; NaturalCron isn't
        // grid-aligned). Cronos / NCrontab are standard cron (`*/6 * * * *`), aligned to the :x6 wall-clock grid.
        var combos = new[]
        {
            new Combo(StaticTimeSpanIntervalTestIdentifier, staticCreatedAtUtc),
            new Combo(StaticNaturalCronTestIdentifier, staticCreatedAtUtc),
            new Combo(AttributeStaticTickTestIdentifier, staticCreatedAtUtc),
            new Combo(dynamicTimeSpanId, dynamicTimeSpanCreatedAtUtc),
            new Combo(dynamicNaturalCronId, dynamicNaturalCronCreatedAtUtc),
            new Combo(dynamicAdvancedId, dynamicAdvancedCreatedAtUtc),
            new Combo(StaticCronosTestIdentifier, staticCreatedAtUtc, GridAligned: true),
            new Combo(StaticNCrontabTestIdentifier, staticCreatedAtUtc, GridAligned: true),
        };

        // Nothing creation-relative has fired yet (catches a "fires immediately" bug). A grid-aligned one may
        // legitimately hit a :x6 mark within the delay.
        await Task.Delay(ImmediateCheckDelay);
        foreach (var combo in combos.Where(c => !c.GridAligned))
        {
            (await runner.Tracker.GetAllAsync(combo.TestIdentifier)).Should().BeEmpty(
                $"'{combo.TestIdentifier}' must not fire within {ImmediateCheckDelay.TotalSeconds}s of being created/started");
        }

        // All combinations tick independently: wait for ≥ 2 firings of each at once.
        var waitTasks = combos.ToDictionary(
            c => c.TestIdentifier,
            c => runner.Tracker.WaitForAsync(c.TestIdentifier, expectedCount: 2, WaitForTwoFiringsTimeout));
        await Task.WhenAll(waitTasks.Values);

        foreach (var combo in combos)
        {
            var executions = await waitTasks[combo.TestIdentifier];
            await AssertOccurrencesAsync(api, clusterId, combo, executions);
        }

        // The dynamic schedules are visible and correct through the API too, not just firing.
        await AssertApiAsync(api, clusterId, dynamicTimeSpanId, dynamicTimeSpan.RecurringScheduleId, TimeSpanIntervalExpressionTypeId, IntervalExpression, "RecurringApp.Tick");
        await AssertApiAsync(api, clusterId, dynamicNaturalCronId, dynamicNaturalCron.RecurringScheduleId, NaturalCronExpressionTypeId, NaturalCronExpression, "RecurringApp.Tick");
        await AssertApiAsync(api, clusterId, dynamicAdvancedId, dynamicAdvanced.RecurringScheduleId, TimeSpanIntervalExpressionTypeId, IntervalExpression, "RecurringApp.AdvancedTick");

        // Cancel the dynamic schedules (the static ones run until the container stops).
        var countAtCancelTimeSpan = (await runner.Tracker.GetAllAsync(dynamicTimeSpanId)).Count;
        var countAtCancelNaturalCron = (await runner.Tracker.GetAllAsync(dynamicNaturalCronId)).Count;
        var countAtCancelAdvanced = (await runner.Tracker.GetAllAsync(dynamicAdvancedId)).Count;
        await recurring.CancelRecurringAsync(dynamicTimeSpan.RecurringScheduleId, clusterId);
        await recurring.CancelRecurringAsync(dynamicNaturalCron.RecurringScheduleId, clusterId);
        await recurring.CancelRecurringAsync(dynamicAdvanced.RecurringScheduleId, clusterId);

        // CancelJobsFromRecurScheduleInactiveOrCanceledRunner ticks every 30 s.
        await Task.Delay(SettleWindowAfterCancel);

        // At most one more execution: a job already in flight at cancel time isn't stopped, only later occurrences.
        (await runner.Tracker.GetAllAsync(dynamicTimeSpanId)).Count.Should().BeLessThanOrEqualTo(countAtCancelTimeSpan + 1,
            "cancelling the dynamic TimeSpanInterval schedule should stop further firings (at most one in-flight execution tolerated)");
        (await runner.Tracker.GetAllAsync(dynamicNaturalCronId)).Count.Should().BeLessThanOrEqualTo(countAtCancelNaturalCron + 1,
            "cancelling the dynamic NaturalCron schedule should stop further firings (at most one in-flight execution tolerated)");
        (await runner.Tracker.GetAllAsync(dynamicAdvancedId)).Count.Should().BeLessThanOrEqualTo(countAtCancelAdvanced + 1,
            "cancelling the dynamic Advanced (JobDefinitionConfigAttribute) schedule should stop further firings (at most one in-flight execution tolerated)");
    }

    // Per execution, from the job's ScheduledAt (the occurrence it was planned for): each occurrence is real and
    // planned once, consecutive ones are exactly one Interval apart (none skipped), the first one is on time, and
    // each job ran close to its occurrence.
    private static async Task AssertOccurrencesAsync(IScenarioApiClient api, string clusterId, Combo combo, IReadOnlyList<ExecutionRecord> executions)
    {
        var name = combo.TestIdentifier;
        executions.Should().HaveCountGreaterThanOrEqualTo(2, $"'{name}' should have fired at least twice");
        executions.Select(e => e.JobId).Should().OnlyHaveUniqueItems($"'{name}' must not run a job twice");

        var runs = new List<(Guid JobId, DateTime ScheduledAt, DateTime ExecutedAt)>();
        foreach (var execution in executions)
        {
            var job = await api.GetJobAsync(clusterId, execution.JobId);
            job.Should().NotBeNull($"'{name}' job {execution.JobId} must be on master");
            job!.ScheduledAt.Should().NotBeNull($"'{name}' job {execution.JobId} must have a ScheduledAt");
            runs.Add((execution.JobId, job.ScheduledAt!.Value, execution.ExecutedAtUtc));
        }

        runs = runs.OrderBy(r => r.ScheduledAt).ToList();
        var details = Describe(runs);

        runs.Select(r => r.ScheduledAt).Should().OnlyHaveUniqueItems(
            $"'{name}' must plan each occurrence once (the same ScheduledAt twice is duplicate planning){details}");

        foreach (var (previous, next) in runs.Zip(runs.Skip(1)))
        {
            (next.ScheduledAt - previous.ScheduledAt - Interval).Duration().Should().BeLessThanOrEqualTo(OccurrenceGapTolerance,
                $"'{name}' consecutive occurrences must be exactly {Interval.TotalMinutes} min apart (none skipped, none extra){details}");
        }

        foreach (var run in runs)
        {
            (run.ExecutedAt - run.ScheduledAt).Should().BeGreaterThanOrEqualTo(-RunEarlyTolerance,
                $"'{name}' job {run.JobId} must not run before its occurrence{details}");
            (run.ExecutedAt - run.ScheduledAt).Should().BeLessThanOrEqualTo(RunLateTolerance,
                $"'{name}' job {run.JobId} must run within {RunLateTolerance.TotalSeconds}s of its occurrence{details}");
        }

        var first = runs[0].ScheduledAt;
        if (combo.GridAligned)
        {
            runs.Should().OnlyContain(r => r.ScheduledAt.Minute % 6 == 0 && r.ScheduledAt.Second == 0,
                $"'{name}' (`*/6 * * * *`) occurrences must be on the :x6 grid{details}");

            // The first :x6 mark after start must not be missed (a slot lost after a start or a deploy); a mark
            // between the real registration and the health check may come first.
            var firstMarkAfterStart = NextGridMark(combo.CreatedAtUtc);
            first.Should().BeOnOrBefore(firstMarkAfterStart,
                $"'{name}' must not miss the first :x6 mark after start ({firstMarkAfterStart:O}){details}");
            first.Should().BeOnOrAfter(combo.CreatedAtUtc - FirstOccurrenceTolerance,
                $"'{name}' first occurrence must come after the schedule was registered{details}");
        }
        else
        {
            (first - (combo.CreatedAtUtc + Interval)).Duration().Should().BeLessThanOrEqualTo(FirstOccurrenceTolerance,
                $"'{name}' first occurrence must be CreatedAt + {Interval.TotalMinutes} min{details}");
        }
    }

    private static DateTime NextGridMark(DateTime utc)
    {
        var minute = new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0, DateTimeKind.Utc);
        var next = minute.AddMinutes(6 - minute.Minute % 6);
        return next;
    }

    private static string Describe(IEnumerable<(Guid JobId, DateTime ScheduledAt, DateTime ExecutedAt)> runs)
    {
        var lines = new List<string> { "", "JobId | ScheduledAt | ExecutedAt" };
        lines.AddRange(runs.Select(r => $"{r.JobId} | {r.ScheduledAt:O} | {r.ExecutedAt:O}"));
        return string.Join(Environment.NewLine, lines);
    }

    private static async Task AssertApiAsync(IScenarioApiClient api, string clusterId, string testIdentifier, Guid recurringScheduleId, string expectedExpressionTypeId, string expectedExpression, string expectedJobDefinitionId)
    {
        var schedules = await api.GetRecurringSchedulesAsync(clusterId, testIdentifier: testIdentifier);
        schedules.Should().ContainSingle(s => GuidBase64.Parse(s.Id) == recurringScheduleId)
            .Which.Should().Match<ApiRecurringSchedule>(s =>
                s.ExpressionTypeId == expectedExpressionTypeId &&
                s.Expression == expectedExpression &&
                s.JobDefinitionId == expectedJobDefinitionId);
    }

    private sealed record Combo(string TestIdentifier, DateTime CreatedAtUtc, bool GridAligned = false);
}
