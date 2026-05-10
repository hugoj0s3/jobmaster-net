using FluentAssertions;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;
using JobMaster.Sdk.Background.Runners.JobsExecution;

namespace JobMaster.UnitTests.Background;

public class TaskQueueControlTests
{
    // ── Create ────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(JobMasterPriority.VeryLow,  2, 10)]
    [InlineData(JobMasterPriority.Low,      3, 15)]
    [InlineData(JobMasterPriority.Medium,   4, 20)]
    [InlineData(JobMasterPriority.High,     5, 25)]
    [InlineData(JobMasterPriority.Critical, 6, 30)]
    public void Create_WhenDefaultFactor_ShouldSizeQueuesByPriority(
        JobMasterPriority priority, int expectedRunSlots, int expectedWaitingCapacity)
    {
        using var sut = TaskQueueControl<string>.Create(priority);

        sut.WaitingQueueCapacity.Should().Be(expectedWaitingCapacity);
        sut.CountAvailability().Should().Be(expectedWaitingCapacity);
        // Running slots are validated indirectly: fill the waiting queue, fill the run slots,
        // and confirm CountRunning matches expectedRunSlots.
        for (var i = 0; i < expectedRunSlots; i++)
            sut.Enqueue(MakeItem());
        sut.StartQueuedTasksIfHasSlotAvailable();
        sut.CountRunning().Should().Be(expectedRunSlots);
    }

    [Fact]
    public void Create_WhenFactorDoubles_ShouldDoubleCapacities()
    {
        // VeryLow base = 2 slots; factor 2.0 → 4 slots, 20 waiting capacity
        using var sut = TaskQueueControl<string>.Create(JobMasterPriority.VeryLow, factor: 2.0);

        sut.WaitingQueueCapacity.Should().Be(20);
    }

    [Fact]
    public void Create_WhenFactorRoundsToZero_ShouldClampRunSlotsToOne()
    {
        // VeryLow base = 2; factor 0.1 → 2 * 0.1 = 0.2 → rounds to 0 → clamped to 1 slot → 5 waiting
        using var sut = TaskQueueControl<string>.Create(JobMasterPriority.VeryLow, factor: 0.1);

        sut.WaitingQueueCapacity.Should().Be(5);
        sut.Enqueue(MakeItem());
        sut.StartQueuedTasksIfHasSlotAvailable();
        sut.CountRunning().Should().Be(1);
    }

    // ── Enqueue ───────────────────────────────────────────────────────────────

    [Fact]
    public void Enqueue_WhenQueueHasSpace_ShouldReturnTrueAndIncrementWaiting()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();

        var result = sut.Enqueue(item);

        result.Should().BeTrue();
        sut.CountWaiting().Should().Be(1);
    }

    [Fact]
    public void Enqueue_WhenWaitingQueueIsFull_ShouldReturnFalse()
    {
        using var sut = CreateSingleSlot(); // 1 slot → 5 waiting capacity

        for (var i = 0; i < sut.WaitingQueueCapacity; i++)
            sut.Enqueue(MakeItem());

        var result = sut.Enqueue(MakeItem()); // 6th item — over capacity

        result.Should().BeFalse();
        sut.CountWaiting().Should().Be(sut.WaitingQueueCapacity);
    }

    [Fact]
    public void Enqueue_WhenIdAlreadyTracked_ShouldReturnTrueWithoutReEnqueueing()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();

        sut.Enqueue(item);
        var result = sut.Enqueue(item); // duplicate

        result.Should().BeTrue();
        sut.CountWaiting().Should().Be(1); // not doubled
    }

    [Fact]
    public void Enqueue_WhenPreEnqueueActionReturnsFalse_ShouldReturnFalseAndNotQueue()
    {
        using var sut = CreateSingleSlot(preEnqueueAction: _ => false);

        var result = sut.Enqueue(MakeItem());

        result.Should().BeFalse();
        sut.CountWaiting().Should().Be(0);
    }

    [Fact]
    public void Enqueue_WhenPreEnqueueActionReturnsTrue_ShouldInvokeActionAndEnqueue()
    {
        var called = false;
        using var sut = CreateSingleSlot(preEnqueueAction: _ => { called = true; return true; });

        var result = sut.Enqueue(MakeItem());

        result.Should().BeTrue();
        called.Should().BeTrue();
        sut.CountWaiting().Should().Be(1);
    }

    [Fact]
    public void Enqueue_WhenIdAlreadyTracked_ShouldNotInvokePreEnqueueActionAgain()
    {
        var callCount = 0;
        using var sut = CreateSingleSlot(preEnqueueAction: _ => { callCount++; return true; });
        var item = MakeItem();

        sut.Enqueue(item);
        sut.Enqueue(item); // duplicate — action must not fire again

        callCount.Should().Be(1);
    }

    [Fact]
    public async Task Enqueue_AfterShutdown_ShouldReturnFalse()
    {
        using var sut = CreateSingleSlot();
        await sut.ShutdownAsync();

        var result = sut.Enqueue(MakeItem());

        result.Should().BeFalse();
    }

    // ── StartQueuedTasksIfHasSlotAvailable ────────────────────────────────────

    [Fact]
    public void StartQueuedTasksIfHasSlotAvailable_WhenItemsWaiting_ShouldPromoteToSlotAndCallStart()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();
        sut.Enqueue(item);

        var result = sut.StartQueuedTasksIfHasSlotAvailable();

        result.Should().BeTrue();
        item.IsStarted.Should().BeTrue();
        item.StartCallCount.Should().Be(1);
        sut.CountWaiting().Should().Be(0);
        sut.CountRunning().Should().Be(1);
    }

    [Fact]
    public void StartQueuedTasksIfHasSlotAvailable_WhenNoItemsWaiting_ShouldReturnFalse()
    {
        using var sut = CreateSingleSlot();

        var result = sut.StartQueuedTasksIfHasSlotAvailable();

        result.Should().BeFalse();
    }

    [Fact]
    public void StartQueuedTasksIfHasSlotAvailable_WhenAllSlotsOccupied_ShouldNotStartNewItem()
    {
        // 1 slot: first item fills it, second must remain waiting
        using var sut = CreateSingleSlot();
        var first  = MakeItem();
        var second = MakeItem();

        sut.Enqueue(first);
        sut.Enqueue(second);
        sut.StartQueuedTasksIfHasSlotAvailable(); // first fills the only slot

        sut.StartQueuedTasksIfHasSlotAvailable(); // slot still busy

        second.IsStarted.Should().BeFalse();
        sut.CountWaiting().Should().Be(1);
        sut.CountRunning().Should().Be(1);
    }

    [Fact]
    public void StartQueuedTasksIfHasSlotAvailable_WhenRunningTaskCompletes_ShouldFreeSlotAndPromoteNext()
    {
        using var sut = CreateSingleSlot();
        var first  = MakeItem();
        var second = MakeItem();

        sut.Enqueue(first);
        sut.Enqueue(second);
        sut.StartQueuedTasksIfHasSlotAvailable(); // first → running, second → waiting

        first.Complete(); // mark task as completed

        var result = sut.StartQueuedTasksIfHasSlotAvailable(); // should clean first, promote second

        result.Should().BeTrue();
        second.IsStarted.Should().BeTrue();
        sut.CountWaiting().Should().Be(0);
        sut.CountRunning().Should().Be(1);
    }

    [Fact]
    public void StartQueuedTasksIfHasSlotAvailable_WhenRunningTaskFaults_ShouldFreeSlot()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();
        sut.Enqueue(item);
        sut.StartQueuedTasksIfHasSlotAvailable();

        item.Fault(new Exception("simulated failure"));
        sut.StartQueuedTasksIfHasSlotAvailable(); // lazy cleanup

        sut.CountRunning().Should().Be(0);
    }

    [Fact]
    public void StartQueuedTasksIfHasSlotAvailable_WhenRunningTaskCancelled_ShouldFreeSlot()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();
        sut.Enqueue(item);
        sut.StartQueuedTasksIfHasSlotAvailable();

        item.Cancel();
        sut.StartQueuedTasksIfHasSlotAvailable(); // lazy cleanup

        sut.CountRunning().Should().Be(0);
    }

    [Fact]
    public void StartQueuedTasksIfHasSlotAvailable_WhenTaskCompletes_ShouldRemoveIdFromTracking()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();
        sut.Enqueue(item);
        sut.StartQueuedTasksIfHasSlotAvailable();

        sut.Contains(item.Id).Should().BeTrue(); // tracked while running

        item.Complete();
        sut.StartQueuedTasksIfHasSlotAvailable(); // triggers lazy cleanup

        sut.Contains(item.Id).Should().BeFalse();
    }

    [Fact]
    public async Task StartQueuedTasksIfHasSlotAvailable_AfterShutdown_ShouldReturnFalse()
    {
        using var sut = CreateSingleSlot();
        sut.Enqueue(MakeItem());
        await sut.ShutdownAsync();

        var result = sut.StartQueuedTasksIfHasSlotAvailable();

        result.Should().BeFalse();
    }

    // ── CountRunning / CountWaiting / CountAvailability ───────────────────────

    [Fact]
    public void CountRunning_ShouldReflectOccupiedSlots()
    {
        using var sut = TaskQueueControl<string>.Create(JobMasterPriority.VeryLow); // 2 slots

        sut.CountRunning().Should().Be(0);

        sut.Enqueue(MakeItem());
        sut.Enqueue(MakeItem());
        sut.StartQueuedTasksIfHasSlotAvailable();

        sut.CountRunning().Should().Be(2);
    }

    [Fact]
    public void CountWaiting_ShouldReflectWaitingQueueSize()
    {
        using var sut = CreateSingleSlot(); // 1 slot, 5 waiting

        sut.CountWaiting().Should().Be(0);

        sut.Enqueue(MakeItem());
        sut.Enqueue(MakeItem());
        sut.CountWaiting().Should().Be(2);

        sut.StartQueuedTasksIfHasSlotAvailable(); // one promoted to running
        sut.CountWaiting().Should().Be(1);
    }

    [Fact]
    public void CountAvailability_ShouldMeasureWaitingQueueSpaceNotRunSlots()
    {
        // VeryLow: 2 run slots, 10 waiting capacity.
        // Availability tracks *waiting queue* space, not run slot vacancies.
        using var sut = TaskQueueControl<string>.Create(JobMasterPriority.VeryLow);

        sut.CountAvailability().Should().Be(10);

        sut.Enqueue(MakeItem());
        sut.CountAvailability().Should().Be(9); // one item in waiting

        sut.StartQueuedTasksIfHasSlotAvailable(); // item promoted to run slot
        sut.CountAvailability().Should().Be(10); // waiting queue freed
    }

    [Fact]
    public async Task CountMethods_AfterShutdown_ShouldAllReturnZero()
    {
        using var sut = CreateSingleSlot(); // 1 slot, 5 waiting
        sut.Enqueue(MakeItem());
        sut.Enqueue(MakeItem());
        sut.StartQueuedTasksIfHasSlotAvailable(); // 1 running, 1 waiting

        await sut.ShutdownAsync();

        sut.CountRunning().Should().Be(0);
        sut.CountWaiting().Should().Be(0);
        sut.CountAvailability().Should().Be(0);
    }

    // ── Contains / GetIds ─────────────────────────────────────────────────────

    [Fact]
    public void Contains_WhenItemInWaitingQueue_ShouldReturnTrue()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();
        sut.Enqueue(item);

        sut.Contains(item.Id).Should().BeTrue();
    }

    [Fact]
    public void Contains_WhenItemPromotedToRunningSlot_ShouldStillReturnTrue()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();
        sut.Enqueue(item);
        sut.StartQueuedTasksIfHasSlotAvailable();

        sut.Contains(item.Id).Should().BeTrue();
    }

    [Fact]
    public void Contains_WhenItemCompletedAndSlotCleaned_ShouldReturnFalse()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();
        sut.Enqueue(item);
        sut.StartQueuedTasksIfHasSlotAvailable();

        item.Complete();
        sut.StartQueuedTasksIfHasSlotAvailable(); // lazy cleanup

        sut.Contains(item.Id).Should().BeFalse();
    }

    [Fact]
    public void Contains_WhenIdNeverEnqueued_ShouldReturnFalse()
    {
        using var sut = CreateSingleSlot();

        sut.Contains(Guid.NewGuid().ToString()).Should().BeFalse();
    }

    [Fact]
    public void GetIds_ShouldReturnAllTrackedIds_AcrossWaitingAndRunning()
    {
        using var sut = CreateSingleSlot(); // 1 slot
        var running = MakeItem();
        var waiting = MakeItem();

        sut.Enqueue(running);
        sut.Enqueue(waiting);
        sut.StartQueuedTasksIfHasSlotAvailable(); // running → slot, waiting → queue

        var ids = sut.GetIds();

        ids.Should().HaveCount(2);
        ids.Should().Contain(running.Id);
        ids.Should().Contain(waiting.Id);
    }

    [Fact]
    public void GetIds_ShouldReturnSnapshot_NotLiveReference()
    {
        using var sut = CreateSingleSlot();
        var item = MakeItem();
        sut.Enqueue(item);

        var snapshot = sut.GetIds();

        sut.Enqueue(MakeItem()); // mutate after snapshot taken

        snapshot.Should().HaveCount(1); // snapshot unaffected
    }

    // ── GetRunningTimeouts ────────────────────────────────────────────────────

    [Fact]
    public void GetRunningTimeouts_WhenNoTasksRunning_ShouldReturnEmpty()
    {
        using var sut = CreateSingleSlot();

        sut.GetRunningTimeouts().Should().BeEmpty();
    }

    [Fact]
    public void GetRunningTimeouts_ShouldReturnTimeoutsOfOccupiedSlotsOnly()
    {
        using var sut = TaskQueueControl<string>.Create(JobMasterPriority.VeryLow); // 2 slots
        var timeout1 = TimeSpan.FromMinutes(2);
        var timeout2 = TimeSpan.FromMinutes(7);
        var item1 = MakeItem(timeout: timeout1);
        var item2 = MakeItem(timeout: timeout2);

        sut.Enqueue(item1);
        sut.Enqueue(item2);
        sut.StartQueuedTasksIfHasSlotAvailable();

        var timeouts = sut.GetRunningTimeouts().ToList();

        timeouts.Should().HaveCount(2);
        timeouts.Should().Contain(timeout1);
        timeouts.Should().Contain(timeout2);
    }

    [Fact]
    public void GetRunningTimeouts_ShouldNotIncludeWaitingItems()
    {
        using var sut = CreateSingleSlot(); // 1 slot
        var runningTimeout = TimeSpan.FromMinutes(3);
        var waitingTimeout = TimeSpan.FromMinutes(9);

        sut.Enqueue(MakeItem(timeout: runningTimeout));
        sut.Enqueue(MakeItem(timeout: waitingTimeout));
        sut.StartQueuedTasksIfHasSlotAvailable(); // first → running, second → waiting

        var timeouts = sut.GetRunningTimeouts().ToList();

        timeouts.Should().ContainSingle().Which.Should().Be(runningTimeout);
        timeouts.Should().NotContain(waitingTimeout);
    }

    // ── ShutdownAsync ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ShutdownAsync_ShouldDrainWaitingQueueAndReturnTheirValues()
    {
        using var sut = CreateSingleSlot(); // 1 slot, 5 waiting
        var running = MakeItem("running-value");
        var waiting1 = MakeItem("waiting-value-1");
        var waiting2 = MakeItem("waiting-value-2");

        sut.Enqueue(running);
        sut.Enqueue(waiting1);
        sut.Enqueue(waiting2);
        sut.StartQueuedTasksIfHasSlotAvailable(); // running → slot; waiting1/2 stay in queue

        running.Complete(); // finish the running task so shutdown doesn't block the full 5 s

        var drained = await sut.ShutdownAsync();

        drained.Should().HaveCount(2);
        drained.Should().Contain("waiting-value-1");
        drained.Should().Contain("waiting-value-2");
        drained.Should().NotContain("running-value"); // running items are not returned
    }

    [Fact]
    public async Task ShutdownAsync_ShouldPreventSubsequentEnqueuesAndCountOps()
    {
        using var sut = CreateSingleSlot();
        await sut.ShutdownAsync();

        sut.Enqueue(MakeItem()).Should().BeFalse();
        sut.StartQueuedTasksIfHasSlotAvailable().Should().BeFalse();
        sut.CountRunning().Should().Be(0);
        sut.CountWaiting().Should().Be(0);
        sut.CountAvailability().Should().Be(0);
    }

    // ── Simulate ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Simulate_EnqueueTask_ShouldExecuteToCompletion()
    {
        // Arrange — real TaskQueueItem with a short async body
        using var sut = CreateSingleSlot();
        var executed = false;

        var item = new TaskQueueItem<string>(
            id: Guid.NewGuid().ToString(),
            value: "my-job",
            timeout: TimeSpan.FromSeconds(5),
            action: async ct =>
            {
                await Task.Delay(50, ct);
                executed = true;
            });

        // Act
        sut.Enqueue(item);
        sut.StartQueuedTasksIfHasSlotAvailable();

        await item.Task; // wait for the real async work to finish

        // Assert
        executed.Should().BeTrue();
        item.Task.IsCompletedSuccessfully.Should().BeTrue();
        item.IsTimedOut().Should().BeFalse();

        // After completion the slot is freed on the next pulse
        sut.StartQueuedTasksIfHasSlotAvailable();
        sut.CountRunning().Should().Be(0);
    }

    [Fact]
    public async Task Simulate_EnqueueTaskThatExceedsTimeout_ShouldBeCancelledAndReportTimedOut()
    {
        // Arrange — timeout shorter than the work the task tries to do
        using var sut = CreateSingleSlot();

        var item = new TaskQueueItem<string>(
            id: Guid.NewGuid().ToString(),
            value: "slow-job",
            timeout: TimeSpan.FromMilliseconds(100),
            action: async ct =>
            {
                // Respects cancellation — would run for 10 s without it
                await Task.Delay(TimeSpan.FromSeconds(10), ct);
            });

        // Act
        sut.Enqueue(item);
        sut.StartQueuedTasksIfHasSlotAvailable();

        // Give the 100 ms timeout enough room to fire
        await Task.WhenAny(item.Task, Task.Delay(TimeSpan.FromSeconds(2)));

        // Assert — CancelAfter(100ms) should have fired, cancelling the delay
        item.Task.IsCanceled.Should().BeTrue();
        item.IsTimedOut().Should().BeTrue();

        // Slot is freed on the next pulse (IsCanceled triggers lazy cleanup)
        sut.StartQueuedTasksIfHasSlotAvailable();
        sut.CountRunning().Should().Be(0);
        sut.Contains(item.Id).Should().BeFalse();
    }

    // ── Dispose ───────────────────────────────────────────────────────────────

    [Fact]
    public void Dispose_WithRunningAndWaitingItems_ShouldNotThrow()
    {
        var sut = CreateSingleSlot();
        sut.Enqueue(MakeItem());
        sut.Enqueue(MakeItem());
        sut.StartQueuedTasksIfHasSlotAvailable();

        var act = () => sut.Dispose();

        act.Should().NotThrow();
    }

    [Fact]
    public void Dispose_WhenCalledTwice_ShouldNotThrow()
    {
        var sut = CreateSingleSlot();
        sut.Dispose();

        var act = () => sut.Dispose();

        act.Should().NotThrow();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a queue with a single running slot (VeryLow priority, factor 0.5 →
    /// rounds to 1 slot, 5 waiting capacity). Keeps slot-boundary tests deterministic.
    /// </summary>
    private static TaskQueueControl<string> CreateSingleSlot(Func<string, bool>? preEnqueueAction = null)
        => TaskQueueControl<string>.Create(JobMasterPriority.VeryLow, factor: 0.5, preEnqueueAction);

    private static FakeTaskQueueItem<string> MakeItem(
        string value = "job",
        TimeSpan? timeout = null)
        => new(value) { Timeout = timeout ?? TimeSpan.FromMinutes(1) };
}

// ── Test double ───────────────────────────────────────────────────────────────

/// <summary>
/// Lightweight <see cref="ITaskQueueItem{T}"/> for unit tests. The underlying task is
/// controlled manually via <see cref="Complete"/>, <see cref="Fault"/>, and
/// <see cref="Cancel"/> so tests can drive slot cleanup without real async work.
/// </summary>
internal sealed class FakeTaskQueueItem<T> : ITaskQueueItem<T>
{
    private readonly TaskCompletionSource _tcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public string Id { get; } = Guid.NewGuid().ToString();
    public T Value { get; }
    public Task Task => _tcs.Task;
    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(1);
    public DateTime EnqueuedAt { get; } = DateTime.UtcNow;
    public DateTime? StartedAt { get; private set; }
    public CancellationTokenSource CancellationTokenSource { get; } = new();

    public bool IsStarted { get; private set; }
    public int StartCallCount { get; private set; }

    public FakeTaskQueueItem(T value) => Value = value;

    public bool IsTimedOut() => false;
    public TimeSpan GetElapsedTime() => DateTime.UtcNow - EnqueuedAt;
    public void Abort() { }

    public void Start()
    {
        StartCallCount++;
        IsStarted = true;
        StartedAt = DateTime.UtcNow;
    }

    /// <summary>Drives the item's task to the <see cref="TaskStatus.RanToCompletion"/> state.</summary>
    public void Complete() => _tcs.TrySetResult();

    /// <summary>Drives the item's task to the <see cref="TaskStatus.Faulted"/> state.</summary>
    public void Fault(Exception ex) => _tcs.TrySetException(ex);

    /// <summary>Drives the item's task to the <see cref="TaskStatus.Canceled"/> state.</summary>
    public void Cancel() => _tcs.TrySetCanceled();

    public void Dispose() { }
}
