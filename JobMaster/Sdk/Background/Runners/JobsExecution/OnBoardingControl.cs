

using System;
using System.Collections.Generic;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Background;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

/// <summary>
/// Generic controller to manage items that need to be held until a specific departure time.
/// Capacity is typically synced with the BackgroundWorker BatchSize for balanced flow.
/// </summary>
internal class OnBoardingControl<T> : IOnBoardingControl<T>
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

    public int CountAvailability()
    {
        lock (syncLock)
        {
            return capacity - holdingPen.Count;
        }
    }

    public int CountItems()
    {
        lock (syncLock)
        {
            return holdingPen.Count;
        }
    }

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
    
    public IList<T> PullPending(int limit)
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

    private void DoPush(T item, string itemId, DateTime departureTime)
    {
        var wrapper = new ItemWrapper(item, itemId, departureTime);
                
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
    
    public bool Contains(string id)
    {
        lock (syncLock)
        {
            return itemIds.Contains(id);
        }
    }

    public IList<string> GetIds()
    {
        lock (syncLock)
        {
            return new List<string>(itemIds);
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

        public ItemWrapper(T item, string id, DateTime departureTime)
        {
            Id = id;
            Item = item;
            DepartureTime = departureTime;
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