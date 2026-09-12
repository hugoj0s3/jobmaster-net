using FluentAssertions;
using JobMaster.Sdk.Background.Runners.JobsExecution;

namespace JobMaster.UnitTests.Background;

public class OnBoardingControlTests
{
    // ── Constructor ───────────────────────────────────────────────────────────

    [Fact]
    public void Constructor_WhenCapacityIsPositive_ShouldUseProvidedCapacity()
    {
        var sut = new OnBoardingControl<string>(capacity: 42);

        sut.CountAvailability().Should().Be(42);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void Constructor_WhenCapacityIsZeroOrNegative_ShouldClampToOneHundred(int badCapacity)
    {
        var sut = new OnBoardingControl<string>(capacity: badCapacity);

        sut.CountAvailability().Should().Be(100);
    }

    // ── Push ──────────────────────────────────────────────────────────────────

    [Fact]
    public void Push_WhenNewItem_ShouldAddToHoldingPenAndTrackId()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        var departure = DateTime.UtcNow.AddMinutes(1);

        sut.Push("job-1", "id-1", departure);

        sut.CountItems().Should().Be(1);
        sut.Contains("id-1").Should().BeTrue();
    }

    [Fact]
    public void Push_WhenDuplicateId_ShouldReplaceExistingEntry()
    {
        // Upsert: pushing the same ID twice must not double-count
        var sut = new OnBoardingControl<string>(capacity: 10);
        var departure = DateTime.UtcNow.AddMinutes(1);

        sut.Push("original", "id-1", departure);
        sut.Push("updated", "id-1", departure.AddMinutes(5));

        sut.CountItems().Should().Be(1);
        sut.Contains("id-1").Should().BeTrue();
    }

    [Fact]
    public void Push_WhenDuplicateId_ShouldUpdateDepartureTime()
    {
        // After an upsert, GetReadyItems should use the NEW departure time
        var sut = new OnBoardingControl<string>(capacity: 10);
        var past = DateTime.UtcNow.AddMinutes(-5);
        var future = DateTime.UtcNow.AddMinutes(10);

        // First push: departure in the past (would be ready now)
        sut.Push("job", "id-1", past);
        // Second push: departure in the future (should NOT be ready now)
        sut.Push("job", "id-1", future);

        var ready = sut.GetReadyItems(DateTime.UtcNow, limit: 10);

        ready.Should().BeEmpty(); // updated to future departure — not due yet
    }

    [Fact]
    public void Push_ShouldInsertItemsInChronologicalOrder()
    {
        // GetReadyItems reads from the front, so earliest must be at index 0
        var sut = new OnBoardingControl<string>(capacity: 10);
        var now = DateTime.UtcNow;

        // Push in reverse chronological order
        sut.Push("c", "id-c", now.AddMinutes(3));
        sut.Push("a", "id-a", now.AddMinutes(1));
        sut.Push("b", "id-b", now.AddMinutes(2));

        // Pull all ready items using a far-future threshold
        var ready = sut.GetReadyItems(now.AddHours(1), limit: 10);

        ready.Should().ContainInOrder("a", "b", "c");
    }

    [Fact]
    public void Push_AfterShutdown_ShouldBeNoOp()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Shutdown();

        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(1));

        sut.CountItems().Should().Be(0);
        sut.Contains("id-1").Should().BeFalse();
    }

    // ── GetReadyItems ─────────────────────────────────────────────────────────

    [Fact]
    public void GetReadyItems_WhenItemsDue_ShouldReturnThemAndRemoveFromPen()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        var past = DateTime.UtcNow.AddMinutes(-1);

        sut.Push("job-1", "id-1", past);
        sut.Push("job-2", "id-2", past);

        var ready = sut.GetReadyItems(DateTime.UtcNow, limit: 10);

        ready.Should().HaveCount(2);
        sut.CountItems().Should().Be(0);
        sut.Contains("id-1").Should().BeFalse();
        sut.Contains("id-2").Should().BeFalse();
    }

    [Fact]
    public void GetReadyItems_WhenNoItemsDue_ShouldReturnEmpty()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(5));

        var ready = sut.GetReadyItems(DateTime.UtcNow, limit: 10);

        ready.Should().BeEmpty();
        sut.CountItems().Should().Be(1); // item remains in pen
    }

    [Fact]
    public void GetReadyItems_ShouldReturnEarliestItemsFirst()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        var now = DateTime.UtcNow;

        sut.Push("late",  "id-late",  now.AddMinutes(-1));
        sut.Push("early", "id-early", now.AddMinutes(-3));
        sut.Push("mid",   "id-mid",   now.AddMinutes(-2));

        var ready = sut.GetReadyItems(now, limit: 10);

        ready.Should().ContainInOrder("early", "mid", "late");
    }

    [Fact]
    public void GetReadyItems_ShouldStopAtFirstNotReadyItem()
    {
        // Mixed: two due, one future. Only the two due must be returned.
        var sut = new OnBoardingControl<string>(capacity: 10);
        var now = DateTime.UtcNow;

        sut.Push("due-1",  "id-1", now.AddMinutes(-2));
        sut.Push("due-2",  "id-2", now.AddMinutes(-1));
        sut.Push("future", "id-3", now.AddMinutes(5));

        var ready = sut.GetReadyItems(now, limit: 10);

        ready.Should().HaveCount(2);
        ready.Should().NotContain("future");
        sut.CountItems().Should().Be(1); // future item still in pen
    }

    [Fact]
    public void GetReadyItems_ShouldRespectLimit()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        var past = DateTime.UtcNow.AddMinutes(-1);

        for (var i = 0; i < 5; i++)
            sut.Push($"job-{i}", $"id-{i}", past);

        var ready = sut.GetReadyItems(DateTime.UtcNow, limit: 3);

        ready.Should().HaveCount(3);
        sut.CountItems().Should().Be(2); // remaining 2 untouched
    }

    [Fact]
    public void GetReadyItems_AfterShutdown_ShouldReturnEmpty()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(-1));
        sut.Shutdown();

        var ready = sut.GetReadyItems(DateTime.UtcNow, limit: 10);

        ready.Should().BeEmpty();
    }

    // ── PullPending ───────────────────────────────────────────────────────────

    [Fact]
    public void PullPending_ShouldReturnLatestDeparturesFirst()
    {
        // PullPending iterates from the back of the sorted list, so it yields the
        // items with the LATEST (furthest future) departure times first.
        var sut = new OnBoardingControl<string>(capacity: 10);
        var now = DateTime.UtcNow;

        sut.Push("early",  "id-early",  now.AddMinutes(1));
        sut.Push("middle", "id-middle", now.AddMinutes(2));
        sut.Push("late",   "id-late",   now.AddMinutes(3));

        var pulled = sut.PullPending(limit: 10);

        pulled.Should().ContainInOrder("late", "middle", "early");
    }

    [Fact]
    public void PullPending_ShouldRespectLimit()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        var now = DateTime.UtcNow;

        for (var i = 0; i < 5; i++)
            sut.Push($"job-{i}", $"id-{i}", now.AddMinutes(i + 1));

        var pulled = sut.PullPending(limit: 2);

        pulled.Should().HaveCount(2);
        sut.CountItems().Should().Be(3);
    }

    [Fact]
    public void PullPending_ShouldRemoveItemsFromTracking()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(1));

        sut.PullPending(limit: 10);

        sut.Contains("id-1").Should().BeFalse();
        sut.CountItems().Should().Be(0);
    }

    [Fact]
    public void PullPending_AfterShutdown_ShouldReturnEmpty()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(1));
        sut.Shutdown();

        var pulled = sut.PullPending(limit: 10);

        pulled.Should().BeEmpty();
    }

    // ── CountAvailability / CountItems ────────────────────────────────────────

    [Fact]
    public void CountAvailability_ShouldDecreaseAsItemsAreAdded()
    {
        var sut = new OnBoardingControl<string>(capacity: 5);

        sut.CountAvailability().Should().Be(5);

        sut.Push("a", "id-a", DateTime.UtcNow.AddMinutes(1));
        sut.CountAvailability().Should().Be(4);

        sut.Push("b", "id-b", DateTime.UtcNow.AddMinutes(2));
        sut.CountAvailability().Should().Be(3);
    }

    [Fact]
    public void CountAvailability_ShouldIncreaseAfterItemsAreDequeued()
    {
        var sut = new OnBoardingControl<string>(capacity: 5);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(-1));

        sut.GetReadyItems(DateTime.UtcNow, limit: 10);

        sut.CountAvailability().Should().Be(5);
    }

    [Fact]
    public void CountItems_ShouldReflectHoldingPenSize()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);

        sut.CountItems().Should().Be(0);

        sut.Push("a", "id-a", DateTime.UtcNow.AddMinutes(1));
        sut.Push("b", "id-b", DateTime.UtcNow.AddMinutes(2));
        sut.CountItems().Should().Be(2);

        sut.PullPending(limit: 1);
        sut.CountItems().Should().Be(1);
    }

    // ── Contains / GetIds ─────────────────────────────────────────────────────

    [Fact]
    public void Contains_WhenItemPushed_ShouldReturnTrue()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(1));

        sut.Contains("id-1").Should().BeTrue();
    }

    [Fact]
    public void Contains_WhenItemNeverPushed_ShouldReturnFalse()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);

        sut.Contains("id-ghost").Should().BeFalse();
    }

    [Fact]
    public void Contains_WhenItemRemovedByGetReadyItems_ShouldReturnFalse()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(-1));

        sut.GetReadyItems(DateTime.UtcNow, limit: 10);

        sut.Contains("id-1").Should().BeFalse();
    }

    [Fact]
    public void Contains_WhenItemRemovedByPullPending_ShouldReturnFalse()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(1));

        sut.PullPending(limit: 10);

        sut.Contains("id-1").Should().BeFalse();
    }

    [Fact]
    public void GetIds_ShouldReturnAllTrackedIds()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("a", "id-a", DateTime.UtcNow.AddMinutes(1));
        sut.Push("b", "id-b", DateTime.UtcNow.AddMinutes(2));

        var ids = sut.GetIds();

        ids.Should().HaveCount(2);
        ids.Should().Contain("id-a");
        ids.Should().Contain("id-b");
    }

    [Fact]
    public void GetIds_ShouldReturnSnapshot_NotLiveReference()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("a", "id-a", DateTime.UtcNow.AddMinutes(1));

        var snapshot = sut.GetIds();
        sut.Push("b", "id-b", DateTime.UtcNow.AddMinutes(2)); // mutate after snapshot

        snapshot.Should().HaveCount(1); // snapshot unaffected
    }

    // ── Shutdown ──────────────────────────────────────────────────────────────

    [Fact]
    public void Shutdown_ShouldReturnAllCurrentItems()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("a", "id-a", DateTime.UtcNow.AddMinutes(1));
        sut.Push("b", "id-b", DateTime.UtcNow.AddMinutes(2));

        var drained = sut.Shutdown();

        drained.Should().HaveCount(2);
        drained.Should().Contain("a");
        drained.Should().Contain("b");
    }

    [Fact]
    public void Shutdown_ShouldClearHoldingPenAndIds()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("a", "id-a", DateTime.UtcNow.AddMinutes(1));

        sut.Shutdown();

        sut.CountItems().Should().Be(0);
        sut.GetIds().Should().BeEmpty();
    }

    [Fact]
    public void Shutdown_ShouldPreventFurtherPush()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Shutdown();

        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(1));

        sut.CountItems().Should().Be(0);
    }

    [Fact]
    public void Shutdown_ShouldPreventGetReadyItems()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(-1));
        sut.Shutdown();

        var ready = sut.GetReadyItems(DateTime.UtcNow, limit: 10);

        ready.Should().BeEmpty();
    }

    [Fact]
    public void Shutdown_ShouldPreventPullPending()
    {
        var sut = new OnBoardingControl<string>(capacity: 10);
        sut.Push("job", "id-1", DateTime.UtcNow.AddMinutes(1));
        sut.Shutdown();

        var pulled = sut.PullPending(limit: 10);

        pulled.Should().BeEmpty();
    }
}
