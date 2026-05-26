using System;
using System.Collections.Generic;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>
/// Concrete implementation of <see cref="IOnBoardingControl{T}"/> that keeps accepted jobs
/// in a time-sorted holding pen until they are ready for execution.
/// </summary>
/// <remarks>
/// <para>
/// Internally the holding pen is a <see cref="List{T}"/> of <c>ItemWrapper</c> objects
/// sorted ascending by <c>DepartureTime</c> using binary search insertion (O(log n)).
/// This means index 0 always holds the item with the earliest departure time, which
/// lets <see cref="GetReadyItems"/> read from the front without scanning the whole list.
/// </para>
/// <para>
/// A parallel <see cref="HashSet{T}"/> of IDs provides O(1) duplicate detection for
/// <see cref="Contains"/> and prevents double-admission in <see cref="Push"/>.
/// </para>
/// <para>
/// All public members are guarded by an internal lock and are safe to call from multiple
/// threads.
/// </para>
/// </remarks>
/// <typeparam name="T">The job model type held by this control.</typeparam>
internal class OnBoardingControl<T> : IOnBoardingControl<T>
{
    private readonly List<ItemWrapper> holdingPen = new List<ItemWrapper>();
    private readonly HashSet<string> itemIds = new HashSet<string>();
    private readonly object syncLock = new object();
    private readonly int capacity;

    private bool isShuttingDown = false;

    /// <summary>
    /// Initializes the control with the given <paramref name="capacity"/>.
    /// </summary>
    /// <param name="capacity">
    /// Maximum number of items the holding pen can hold. Values ≤ 0 are clamped to
    /// 100 to guard against misconfigured worker settings.
    /// </param>
    public OnBoardingControl(int capacity)
    {
        // Ensures a valid capacity even if the BatchSize is misconfigured.
        this.capacity = capacity > 0 ? capacity : 100;
    }

    /// <inheritdoc/>
    public int CountAvailability()
    {
        lock (syncLock)
        {
            return capacity - holdingPen.Count;
        }
    }

    /// <inheritdoc/>
    public int CountItems()
    {
        lock (syncLock)
        {
            return holdingPen.Count;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Upsert: if <paramref name="id"/> already exists the old entry is removed before
    /// the new one is inserted, effectively updating its departure time.
    /// </remarks>
    public void Push(T item, string id, DateTime departureTime)
    {
        lock (syncLock)
        {
            if (isShuttingDown) return;

            if (itemIds.Contains(id))
            {
                var existingItem = this.holdingPen.Find(x => x.Id == id);
                this.holdingPen.Remove(existingItem!);
                itemIds.Remove(id);
            }

            DoPush(item, id, departureTime);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Iterates from the <b>end</b> of the sorted list (latest departure times first).
    /// This is intentional for bulk-flush scenarios where chronological order does not
    /// matter and we want the items that are furthest from being due.
    /// </remarks>
    public IList<T> PullPending(int limit)
    {
        List<T> pruneItems = new List<T>();
        lock (syncLock)
        {
            if (isShuttingDown) return pruneItems;

            for (int i = holdingPen.Count - 1; i >= 0; i--)
            {
                pruneItems.Add(holdingPen[i].Item);
                itemIds.Remove(holdingPen[i].Id);
                holdingPen.RemoveAt(i);

                if (pruneItems.Count >= limit)
                {
                    break;
                }
            }
        }

        return pruneItems;
    }

    /// <summary>
    /// Inserts <paramref name="item"/> into the sorted holding pen using binary search,
    /// maintaining ascending order by <c>DepartureTime</c>.
    /// </summary>
    private void DoPush(T item, string itemId, DateTime departureTime)
    {
        var wrapper = new ItemWrapper(item, itemId, departureTime);

        // O(log n) efficiency ensures zero performance impact during high-frequency pushes.
        int index = holdingPen.BinarySearch(wrapper, new DepartureComparer());

        if (index < 0) index = ~index;

        holdingPen.Insert(index, wrapper);
        itemIds.Add(itemId);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Reads from the <b>front</b> of the sorted list (earliest departure times first) and
    /// stops as soon as the next item has a future departure time, so the scan is O(k)
    /// where k is the number of ready items rather than the full list size.
    /// </remarks>
    public IList<T> GetReadyItems(DateTime now, int limit)
    {
        List<T> result = new List<T>();

        lock (syncLock)
        {
            if (isShuttingDown) return result;

            int count = 0;
            // Since the list is sorted, we only ever evaluate the head.
            while (holdingPen.Count > 0 && count < limit)
            {
                if (holdingPen[0].DepartureTime <= now)
                {
                    var wrapper = holdingPen[0];
                    result.Add(wrapper.Item);
                    itemIds.Remove(wrapper.Id);
                    holdingPen.RemoveAt(0);
                    count++;
                }
                else
                {
                    break; // Stop immediately once the next item is not yet ready.
                }
            }
        }

        return result;
    }

    /// <inheritdoc/>
    public bool Contains(string id)
    {
        lock (syncLock)
        {
            return itemIds.Contains(id);
        }
    }

    /// <inheritdoc/>
    public IList<string> GetIds()
    {
        lock (syncLock)
        {
            return new List<string>(itemIds);
        }
    }

    /// <inheritdoc/>
    public IList<T> Shutdown()
    {
        lock (syncLock)
        {
            isShuttingDown = true;
            var result = holdingPen.Select(x => x.Item).ToList();
            holdingPen.Clear();
            itemIds.Clear();
            return result;
        }
    }

    // ── Private types ─────────────────────────────────────────────────────────

    /// <summary>Wraps a held item together with its ID and scheduled departure time.</summary>
    private class ItemWrapper
    {
        public string Id { get; set; }
        public T Item { get; set; }
        public DateTime DepartureTime { get; set; }

        public ItemWrapper(T item, string id, DateTime departureTime)
        {
            Id = id;
            Item = item;
            DepartureTime = departureTime;
        }
    }

    /// <summary>
    /// Compares <see cref="ItemWrapper"/> instances by <see cref="ItemWrapper.DepartureTime"/>
    /// in ascending order so that binary search insertion keeps the holding pen sorted with
    /// the earliest departure at index 0.
    /// </summary>
    private class DepartureComparer : IComparer<ItemWrapper>
    {
        public int Compare(ItemWrapper? x, ItemWrapper? y)
        {
            if (x == null || y == null)
                return 0;

            // Chronological sorting: earliest DepartureTime comes first.
            return x.DepartureTime.CompareTo(y.DepartureTime);
        }
    }
}
