using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioMatrixTests.Scenarios.ConfirmOnMasterTest;

/// <summary>
/// A distributed cluster (agent connections, so receipts are <c>AcceptedOnTransport</c>) where the app confirms on
/// master right after scheduling (<c>Advanced.ConfirmOnMasterAsync(receipts)</c> for jobs,
/// <c>receipt.ConfirmOnMasterAsync()</c> for the recurring schedule). Checks the confirm's guarantee -- everything is on
/// master as soon as the call returns -- and that the race with the agent's own save never runs anything twice or
/// loses anything. Then the same race for <c>receipt.CancelAsync()</c> (a cancel that returned true never runs), and
/// cancel by id once the item is on master. See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class ConfirmOnMasterTests(ScenarioGlobalEnvironment global)
{
    private const int QtyJobs = 300;
    private const string TimeSpanIntervalExpressionTypeId = "TimeSpanInterval";
    private const string RecurringInterval = "00:00:15";
    private const int MinRecurringRuns = 3;

    private const int QtyCancelJobs = 50;

    // Mirror JobMasterJobStatus.Succeeded / Cancelled and RecurringScheduleStatus.Canceled (no JobMaster reference here).
    private const int SucceededStatus = 5;
    private const int CancelledStatus = 8;
    private const int CanceledStatus = 3;

    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(3);
    private static readonly TimeSpan CancelLaterDueIn = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan CancelSettleMargin = TimeSpan.FromSeconds(45);

    public static IEnumerable<object[]> Flavors() =>
        ScenarioFlavors.AllExcept().Select(flavor => new object[] { flavor.Name });

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Run(string flavor)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "ConfirmOnMasterTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(json, selected));
        await runner.StartPhaseAsync("phase-1");

        var clusterId = $"mx-{selected.Code}-confirm";
        var containerName = clusterId;
        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        // One-off jobs, confirmed as a batch right after scheduling.
        var jobsTestIdentifier = Guid.NewGuid().ToString("N");
        var scheduled = await runner.ScheduleFor(containerName)
            .ScheduleAsync("fast", jobsTestIdentifier, qtyJobs: QtyJobs, clusterId: clusterId, confirmOnMaster: true);
        scheduled.JobIds.Should().HaveCount(QtyJobs);
        scheduled.AcceptedOnTransportCount.Should().BeGreaterThan(0,
            "the cluster has live agent connections, so receipts should be AcceptedOnTransport -- otherwise the confirm had nothing to do");

        // The guarantee: on master when the call returns (no waiting for the agents' own saves).
        (await api.GetJobCountAsync(clusterId, testIdentifier: jobsTestIdentifier)).Should().Be(QtyJobs,
            "every confirmed job must be on master as soon as ConfirmOnMasterAsync returns");

        // Each job runs exactly once, whichever of the confirm or the agent's save won.
        var executions = await runner.Tracker.WaitForAsync(jobsTestIdentifier, QtyJobs, ExecuteWaitTimeout);
        executions.Select(e => e.JobId).Should().BeEquivalentTo(scheduled.JobIds);

        // Recurring schedule, confirmed right after creating it.
        var recurringTestIdentifier = Guid.NewGuid().ToString("N");
        var recurring = await runner.RecurringScheduleFor(containerName)
            .CreateRecurringAsync("fast", TimeSpanIntervalExpressionTypeId, RecurringInterval, recurringTestIdentifier,
                clusterId: clusterId, confirmOnMaster: true);
        recurring.AcceptedOnTransport.Should().BeTrue(
            "the cluster has live agent connections, so the recurring receipt should be AcceptedOnTransport");

        var schedules = await api.GetRecurringSchedulesAsync(clusterId, recurringTestIdentifier);
        schedules.Should().ContainSingle("the confirmed recurring schedule must be on master as soon as the call returns");
        GuidBase64.Parse(schedules[0].Id).Should().Be(recurring.RecurringScheduleId);

        // Planned and firing (the agent's duplicate save must not stall it), one job per occurrence.
        var recurringRuns = await runner.Tracker.WaitForAsync(recurringTestIdentifier, MinRecurringRuns, ExecuteWaitTimeout);
        recurringRuns.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        await runner.RecurringScheduleFor(containerName).CancelRecurringAsync(recurring.RecurringScheduleId, clusterId: clusterId);

        // Settle, then: no late duplicate execution, and every one-off job finished on master.
        await Task.Delay(TimeSpan.FromSeconds(5));
        var allExecutions = await runner.Tracker.GetAllAsync(jobsTestIdentifier);
        allExecutions.Should().HaveCount(QtyJobs);
        allExecutions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        await PollingWaitUtil.WaitUntilAsync(TimeSpan.FromMinutes(2),
            async () => await api.GetJobCountAsync(clusterId, testIdentifier: jobsTestIdentifier, status: SucceededStatus) == QtyJobs,
            "every confirmed job to be Succeeded on master",
            pollInterval: TimeSpan.FromSeconds(2));

        await RunCancelAsync(runner, api, clusterId, containerName);
    }

    // receipt.CancelAsync() right after scheduling, while the receipts are still AcceptedOnTransport: whichever of the
    // cancel or the agent's save reaches master first, a cancel that returned true must never run. Plus cancel by id
    // once the item is on master.
    private static async Task RunCancelAsync(ScenarioRunner runner, IScenarioApiClient api, string clusterId, string containerName)
    {
        // Due well after the ~50s window: every cancel must succeed (inserted cancelled, or cancelled by id once the
        // agent saved it).
        var laterTestIdentifier = Guid.NewGuid().ToString("N");
        var later = await runner.ScheduleFor(containerName)
            .ScheduleAsync("fast", laterTestIdentifier, qtyJobs: QtyCancelJobs, clusterId: clusterId,
                afterSeconds: (int)CancelLaterDueIn.TotalSeconds, cancelFromReceipt: true);
        later.AcceptedOnTransportCount.Should().BeGreaterThan(0,
            "the cluster has live agent connections, so receipts should be AcceptedOnTransport");
        later.CancelledJobIds.Should().BeEquivalentTo(later.JobIds,
            "jobs due after the onboarding window can always be cancelled from the receipt");
        var laterDueAt = DateTime.UtcNow.Add(CancelLaterDueIn);

        // Cancel by id (the app retries until the agent saved it on master): same outcome once it's there.
        var byIdTestIdentifier = Guid.NewGuid().ToString("N");
        var byId = await runner.ScheduleFor(containerName)
            .ScheduleAsync("fast", byIdTestIdentifier, qtyJobs: QtyCancelJobs, clusterId: clusterId,
                afterSeconds: (int)CancelLaterDueIn.TotalSeconds, cancelFromId: true);
        byId.CancelledJobIds.Should().BeEquivalentTo(byId.JobIds,
            "jobs due after the onboarding window can always be cancelled by id once they're on master");
        var byIdDueAt = DateTime.UtcNow.Add(CancelLaterDueIn);

        // Due now: a cancel may lose the race with the agent's save and the onboarding window (false, it runs);
        // only the ones that returned true are checked.
        var nowTestIdentifier = Guid.NewGuid().ToString("N");
        var now = await runner.ScheduleFor(containerName)
            .ScheduleAsync("fast", nowTestIdentifier, qtyJobs: QtyCancelJobs, clusterId: clusterId, cancelFromReceipt: true);
        var nowCancelled = now.CancelledJobIds ?? new List<Guid>();
        var nowNotCancelled = now.JobIds.Except(nowCancelled).ToList();

        var recurringTestIdentifier = Guid.NewGuid().ToString("N");
        var recurring = await runner.RecurringScheduleFor(containerName)
            .CreateRecurringAsync("fast", TimeSpanIntervalExpressionTypeId, RecurringInterval, recurringTestIdentifier,
                clusterId: clusterId, cancelFromReceipt: true);
        recurring.Cancelled.Should().BeTrue("a just-created schedule can always be cancelled from its receipt");

        var recurringByIdTestIdentifier = Guid.NewGuid().ToString("N");
        var recurringById = await runner.RecurringScheduleFor(containerName)
            .CreateRecurringAsync("fast", TimeSpanIntervalExpressionTypeId, RecurringInterval, recurringByIdTestIdentifier,
                clusterId: clusterId, cancelFromId: true);
        recurringById.Cancelled.Should().BeTrue("an active schedule can always be cancelled by id once it's on master");

        // The ones not cancelled still run, exactly once.
        if (nowNotCancelled.Count > 0)
        {
            var ran = await runner.Tracker.WaitForAsync(nowTestIdentifier, nowNotCancelled.Count, ExecuteWaitTimeout);
            ran.Select(e => e.JobId).Should().BeEquivalentTo(nowNotCancelled);
        }

        // Past the later jobs' due time (+ margin for the agent's late save and assignment), nothing cancelled ran.
        var waitUntil = (byIdDueAt > laterDueAt ? byIdDueAt : laterDueAt).Add(CancelSettleMargin);
        if (waitUntil > DateTime.UtcNow)
        {
            await Task.Delay(waitUntil - DateTime.UtcNow);
        }

        (await runner.Tracker.GetAllAsync(laterTestIdentifier)).Should().BeEmpty("cancelled jobs must never run");
        (await runner.Tracker.GetAllAsync(nowTestIdentifier)).Select(e => e.JobId).Should().NotIntersectWith(nowCancelled,
            "a job whose receipt cancel returned true must never run");
        (await runner.Tracker.GetAllAsync(recurringTestIdentifier)).Should().BeEmpty("a cancelled schedule must never fire");
        (await runner.Tracker.GetAllAsync(byIdTestIdentifier)).Should().BeEmpty("jobs cancelled by id must never run");
        (await runner.Tracker.GetAllAsync(recurringByIdTestIdentifier)).Should().BeEmpty(
            "a schedule cancelled by id right after it reached master must never fire");

        // And master holds them cancelled (the agent's late save didn't revive them).
        (await api.GetJobCountAsync(clusterId, testIdentifier: laterTestIdentifier, status: CancelledStatus)).Should().Be(QtyCancelJobs);
        (await api.GetJobCountAsync(clusterId, testIdentifier: byIdTestIdentifier, status: CancelledStatus)).Should().Be(QtyCancelJobs);
        var schedules = await api.GetRecurringSchedulesAsync(clusterId, recurringTestIdentifier);
        schedules.Should().ContainSingle().Which.Status.GetInt32().Should().Be(CanceledStatus);
        var schedulesById = await api.GetRecurringSchedulesAsync(clusterId, recurringByIdTestIdentifier);
        schedulesById.Should().ContainSingle().Which.Status.GetInt32().Should().Be(CanceledStatus);
    }
}
