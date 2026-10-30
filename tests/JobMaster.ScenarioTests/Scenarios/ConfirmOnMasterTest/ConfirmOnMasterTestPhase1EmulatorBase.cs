using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;
using JobMaster.ScenarioTests.Scenarios.ScheduleTest;

namespace JobMaster.ScenarioTests.Scenarios.ConfirmOnMasterTest;

/// <summary>
/// Shared phase-1 logic for every repo-type variant: a distributed cluster (agent connections, so receipts are
/// <c>AcceptedOnTransport</c>) where the app confirms on master right after scheduling
/// (<c>Advanced.ConfirmOnMasterAsync(receipts)</c> for jobs, <c>receipt.ConfirmOnMasterAsync()</c> for the recurring
/// schedule). Checks the confirm's guarantee -- everything is on master as soon as the call returns -- and that the
/// race with the agent's own save (one of them gets a duplicate) never runs anything twice or loses anything.
/// Then the same race for <c>receipt.CancelAsync()</c>: a cancel that returned true never runs.
/// </summary>
public abstract class ConfirmOnMasterTestPhase1EmulatorBase<TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TPhaseEnum : struct, Enum
{
    private const int QtyJobs = 300;
    private const string TimeSpanIntervalExpressionTypeId = "TimeSpanInterval";
    private const string RecurringInterval = "00:00:15";
    private const int MinRecurringRuns = 3;

    private const int QtyCancelJobs = 50;

    // Mirror JobMasterJobStatus.Succeeded / Cancelled and RecurringScheduleStatus.Canceled (no JobMaster reference in this project).
    private const int SucceededStatus = 5;
    private const int CancelledStatus = 8;
    private const int CanceledStatus = 3;

    private static readonly TimeSpan ExecuteWaitTimeout = TimeSpan.FromMinutes(3);
    private static readonly TimeSpan CancelLaterDueIn = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan CancelSettleMargin = TimeSpan.FromSeconds(45);

    protected abstract string ClusterId { get; }
    protected abstract string ContainerName { get; }

    public override async Task RunAsync()
    {
        var api = Runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");

        // One-off jobs, confirmed as a batch right after scheduling.
        var jobsTestIdentifier = Guid.NewGuid().ToString("N");
        var scheduled = await Runner.ScheduleFor(ContainerName)
            .ScheduleAsync("fast", jobsTestIdentifier, qtyJobs: QtyJobs, clusterId: ClusterId, confirmOnMaster: true);
        scheduled.JobIds.Should().HaveCount(QtyJobs);
        scheduled.AcceptedOnTransportCount.Should().BeGreaterThan(0,
            "the cluster has live agent connections, so receipts should be AcceptedOnTransport -- otherwise the confirm had nothing to do");

        // The guarantee: on master when the call returns (no waiting for the agents' own saves).
        (await api.GetJobCountAsync(ClusterId, testIdentifier: jobsTestIdentifier)).Should().Be(QtyJobs,
            "every confirmed job must be on master as soon as ConfirmOnMasterAsync returns");

        // Each job runs exactly once, whichever of the confirm or the agent's save won.
        var executions = await Runner.Tracker.WaitForAsync(jobsTestIdentifier, QtyJobs, ExecuteWaitTimeout);
        executions.Select(e => e.JobId).Should().BeEquivalentTo(scheduled.JobIds);

        // Recurring schedule, confirmed right after creating it.
        var recurringTestIdentifier = Guid.NewGuid().ToString("N");
        var recurring = await Runner.RecurringScheduleFor(ContainerName)
            .CreateRecurringAsync("fast", TimeSpanIntervalExpressionTypeId, RecurringInterval, recurringTestIdentifier,
                clusterId: ClusterId, confirmOnMaster: true);
        recurring.AcceptedOnTransport.Should().BeTrue(
            "the cluster has live agent connections, so the recurring receipt should be AcceptedOnTransport");

        var schedules = await api.GetRecurringSchedulesAsync(ClusterId, recurringTestIdentifier);
        schedules.Should().ContainSingle("the confirmed recurring schedule must be on master as soon as the call returns");
        GuidBase64.Parse(schedules[0].Id).Should().Be(recurring.RecurringScheduleId);

        // Planned and firing (the agent's duplicate save must not stall it), one job per occurrence.
        var recurringRuns = await Runner.Tracker.WaitForAsync(recurringTestIdentifier, MinRecurringRuns, ExecuteWaitTimeout);
        recurringRuns.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        await Runner.RecurringScheduleFor(ContainerName).CancelRecurringAsync(recurring.RecurringScheduleId, clusterId: ClusterId);

        // Settle, then: no late duplicate execution, and every one-off job finished on master.
        await Task.Delay(TimeSpan.FromSeconds(5));
        var allExecutions = await Runner.Tracker.GetAllAsync(jobsTestIdentifier);
        allExecutions.Should().HaveCount(QtyJobs);
        allExecutions.Select(e => e.JobId).Should().OnlyHaveUniqueItems();

        await PollingWaitUtil.WaitUntilAsync(TimeSpan.FromMinutes(2),
            async () => await api.GetJobCountAsync(ClusterId, testIdentifier: jobsTestIdentifier, status: SucceededStatus) == QtyJobs,
            "every confirmed job to be Succeeded on master",
            pollInterval: TimeSpan.FromSeconds(2));

        await RunCancelFromReceiptAsync(api);
    }

    // receipt.CancelAsync() right after scheduling, while the receipts are still AcceptedOnTransport: whichever of the
    // cancel or the agent's save reaches master first, a cancel that returned true must never run.
    private async Task RunCancelFromReceiptAsync(IScenarioApiClient api)
    {
        // Due well after the ~50s window: every cancel must succeed (inserted cancelled, or cancelled by id once the
        // agent saved it).
        var laterTestIdentifier = Guid.NewGuid().ToString("N");
        var later = await Runner.ScheduleFor(ContainerName)
            .ScheduleAsync("fast", laterTestIdentifier, qtyJobs: QtyCancelJobs, clusterId: ClusterId,
                afterSeconds: (int)CancelLaterDueIn.TotalSeconds, cancelFromReceipt: true);
        later.AcceptedOnTransportCount.Should().BeGreaterThan(0,
            "the cluster has live agent connections, so receipts should be AcceptedOnTransport");
        later.CancelledJobIds.Should().BeEquivalentTo(later.JobIds,
            "jobs due after the onboarding window can always be cancelled from the receipt");
        var laterDueAt = DateTime.UtcNow.Add(CancelLaterDueIn);

        // Due now: a cancel may lose the race with the agent's save and the onboarding window (false, it runs);
        // only the ones that returned true are checked.
        var nowTestIdentifier = Guid.NewGuid().ToString("N");
        var now = await Runner.ScheduleFor(ContainerName)
            .ScheduleAsync("fast", nowTestIdentifier, qtyJobs: QtyCancelJobs, clusterId: ClusterId, cancelFromReceipt: true);
        var nowCancelled = now.CancelledJobIds ?? new List<Guid>();
        var nowNotCancelled = now.JobIds.Except(nowCancelled).ToList();

        var recurringTestIdentifier = Guid.NewGuid().ToString("N");
        var recurring = await Runner.RecurringScheduleFor(ContainerName)
            .CreateRecurringAsync("fast", TimeSpanIntervalExpressionTypeId, RecurringInterval, recurringTestIdentifier,
                clusterId: ClusterId, cancelFromReceipt: true);
        recurring.Cancelled.Should().BeTrue("a just-created schedule can always be cancelled from its receipt");

        // The ones not cancelled still run, exactly once.
        if (nowNotCancelled.Count > 0)
        {
            var ran = await Runner.Tracker.WaitForAsync(nowTestIdentifier, nowNotCancelled.Count, ExecuteWaitTimeout);
            ran.Select(e => e.JobId).Should().BeEquivalentTo(nowNotCancelled);
        }

        // Past the later jobs' due time (+ margin for the agent's late save and assignment), nothing cancelled ran.
        var waitUntil = laterDueAt.Add(CancelSettleMargin);
        if (waitUntil > DateTime.UtcNow)
        {
            await Task.Delay(waitUntil - DateTime.UtcNow);
        }

        (await Runner.Tracker.GetAllAsync(laterTestIdentifier)).Should().BeEmpty("cancelled jobs must never run");
        (await Runner.Tracker.GetAllAsync(nowTestIdentifier)).Select(e => e.JobId).Should().NotIntersectWith(nowCancelled,
            "a job whose receipt cancel returned true must never run");
        (await Runner.Tracker.GetAllAsync(recurringTestIdentifier)).Should().BeEmpty("a cancelled schedule must never fire");

        // And master holds them cancelled (the agent's late save didn't revive them).
        (await api.GetJobCountAsync(ClusterId, testIdentifier: laterTestIdentifier, status: CancelledStatus)).Should().Be(QtyCancelJobs);
        var schedules = await api.GetRecurringSchedulesAsync(ClusterId, recurringTestIdentifier);
        schedules.Should().ContainSingle().Which.Status.GetInt32().Should().Be(CanceledStatus);
    }
}
