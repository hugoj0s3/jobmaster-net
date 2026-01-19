

using System;
using System.Collections.Generic;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Background;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>
/// Generic controller to manage items that need to be held until a specific departure time.
/// Capacity is typically synced with the BackgroundWorker BatchSize for balanced flow.
/// </summary>
public class OnBoardingControl<T> : IOnBoardingControl<T>
{
    private readonly List<ItemWrapper> holdingPen = new List<ItemWrapper>();
    private readonly HashSet<string> itemIds = new HashSet<string>();
    private readonly object syncLock = new object();
    private readonly int capacity;
    
    private bool isShuttingDown = false;

    /// <summary>
    /// Initializes the control with a fixed capacity derived from the Worker configuration.
    /// </summary>
    public OnBoardingControl(int capacity)
    {
        // Ensures a valid capacity even if the BatchSize is misconfigured.
        this.capacity = capacity > 0 ? capacity : 100;
    }

    public bool Contains(string id)
    {
        lock (syncLock)
        {
            return itemIds.Contains(id);
        }
    }

    public int CountAvailability()
    {
        lock (syncLock)
        {
            return capacity - holdingPen.Count;
        }
    }

    /// <summary>
    /// Pushes an item into the pen. Maintains chronological order via Binary Search.
    /// Returns: true if added, false if duplicate or capacity full.
    /// Use IsDuplicate() to distinguish between duplicate vs capacity issues.
    /// </summary>
    public bool Push(T item, string id, DateTime departureTime, DateTime departureDeadline)
    {
        lock (syncLock)
        {
            if (isShuttingDown) return false;

            if (departureDeadline < JobMasterConstants.NowUtcWithSkewTolerance())
            {
                return false;
            }
            
            if (itemIds.Contains(id))
            {
                var existingItem = this.holdingPen.Find(x => x.Id == id);
                // replace and return true.
                this.holdingPen.Remove(existingItem!);
                itemIds.Remove(id);
                DoPush(item, id, departureTime, departureDeadline);
                    
                return true;
            }
            
            if (holdingPen.Count >= capacity)
                return false; // Capacity full

            DoPush(item, id, departureTime, departureDeadline);
            return true;
        }
    }

    public void ForcePush(T item, string id, DateTime departureTime, DateTime departureDeadline)
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

            DoPush(item, id, departureTime, departureDeadline);
        }
    }
    
    public int Count()
    {
        lock (syncLock)
        {
            return holdingPen.Count;
        }
    }
    
    public IList<T> PruneDeadlinedItems()
    {
        List<T> expiredItems = new List<T>();

        lock (syncLock)
        {
            if (isShuttingDown) return expiredItems;

            // Iterating backwards to safely remove items while traversing
            for (int i = holdingPen.Count - 1; i >= 0; i--)
            {
                if (holdingPen[i].DepartureDeadline < JobMasterConstants.NowUtcWithSkewTolerance())
                {
                    var wrapper = holdingPen[i];
                    expiredItems.Add(wrapper.Item);
                    itemIds.Remove(wrapper.Id);
                    holdingPen.RemoveAt(i);
                }
            }
        }

        return expiredItems;
    }

    public IList<T> PruneOldDepartureItems(int limit)
    {
        List<T> pruneItems = new List<T>();
        lock(syncLock)
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

    private void DoPush(T item, string itemId, DateTime departureTime, DateTime departureDeadline)
    {
        var wrapper = new ItemWrapper(item, itemId, departureTime, departureDeadline);
                
        // O(log n) efficiency ensures zero performance impact during high-frequency pushes.
        int index = holdingPen.BinarySearch(wrapper, new DepartureComparer());
                
        if (index < 0) index = ~index;
                
        holdingPen.Insert(index, wrapper);
        itemIds.Add(itemId);
    }

    /// <summary>
    /// Retrieves items ready for departure. Processes "most close" items first.
    /// </summary>
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
    
    /// <summary>
    /// Returns the earliest departure time among buffered items, if any.
    /// </summary>
    public DateTime? PeekNextDepartureTime()
    {
        lock (syncLock)
        {
            if (holdingPen.Count == 0)
                return null;

            return holdingPen[0].DepartureTime;
        }
    }

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
        
    private class ItemWrapper
    {
        public string Id { get; set; }
        public T Item { get; set; }
        public DateTime DepartureTime { get; set; }
        
        public DateTime DepartureDeadline { get; set; }

        public ItemWrapper(T item, string id, DateTime departureTime, DateTime departureDeadline)
        {
            Id = id;
            Item = item;
            DepartureTime = departureTime;
            DepartureDeadline = departureDeadline;
        }
    }
        
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