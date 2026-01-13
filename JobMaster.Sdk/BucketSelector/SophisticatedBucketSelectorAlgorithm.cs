using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts.BucketSelector;
using JobMaster.Sdk.Contracts.Models.Buckets;

namespace JobMaster.Sdk.BucketSelector;

[Obsolete]
public class SophisticatedBucketSelectorAlgorithm : IBucketSelectorAlgorithm
{
    private readonly Dictionary<string, BucketSelectionStats> bucketSelectionStats 
        = new();
    
    private string? lastSelectedBucketId;
    private DateTime? lastSelectionTime;
    private int picksSinceRandom = 0;

    // Tunables
    private const int RandomEveryNCycles = 10;
    private static readonly TimeSpan Cooldown = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan NewBucketBoostWindow = TimeSpan.FromMinutes(5);
    private const double NewBucketBoostProbability = 0.75; // 75% chance to boost very new buckets
    private const double CooldownRandomProbability = 0.75;  // 75% chance to use random during cooldown

    public BucketModel? Select(IList<BucketModel> availableBuckets)
    {
        // Remove buckets that are older than 24 hours since creation and have been selected less than once in the last 2 hours
        CleanUpBucketSelectionStats(availableBuckets);
        RegisterAvailableBuckets(availableBuckets);
        
        if (availableBuckets.Count <= 0)
        {
            return null;
        }
        
        if (availableBuckets.Count == 1)
        {
            IncrementBucketSelectionStats(availableBuckets[0].Id);
            return availableBuckets[0];
        }

        if (picksSinceRandom >= RandomEveryNCycles)
        {
            var randomBucket = RandomSelect(availableBuckets);
            if (randomBucket is not null)
            {
                picksSinceRandom = 0;
                return randomBucket;
            }
        }

        var now = DateTime.UtcNow;

        if (lastSelectionTime is not null && 
            now - lastSelectionTime < Cooldown && JobMasterRandomUtil.GetBoolean(CooldownRandomProbability))
        {
            var randomBucket = RandomSelect(availableBuckets);
            if (randomBucket is not null)
            {
                return randomBucket;
            }
        }   

        // Single-pass selection with skip-last and tuple tie-breakers
        BucketModel? best = null;
        (int sel, DateTime last, long createdNegTicks) bestKey = default;
        
        for (int i = 0; i < availableBuckets.Count; i++)
        {
            var b = availableBuckets[i];
            if (lastSelectedBucketId != null && b.Id == lastSelectedBucketId)
                continue;

            // Early boost for very recent buckets (help ramp-up new capacity)
            if (now - b.CreatedAt <= NewBucketBoostWindow && JobMasterRandomUtil.GetBoolean(NewBucketBoostProbability))
            {
                IncrementBucketSelectionStats(b.Id);
                return b;
            }

            var s = bucketSelectionStats[b.Id];
            var key = (s.SelectionCount, s.LastSelectionTime, -b.CreatedAt.Ticks);

            if (best == null || key.CompareTo(bestKey) < 0)
            {
                best = b;
                bestKey = key;
            }
        }

        // Fallback if all were skipped (shouldn't happen with Count>1, but be safe)
        if (best == null)
        {
            best = availableBuckets[0];
        }

        IncrementBucketSelectionStats(best.Id);
        return best;
    }

    private BucketModel? RandomSelect(IList<BucketModel> availableBuckets)
    {
        var randomBucket = availableBuckets.Random();
        
        if (randomBucket is null) 
            return null;
        
        if (lastSelectedBucketId is not null && randomBucket.Id == lastSelectedBucketId) 
            return null;
        
        IncrementBucketSelectionStats(randomBucket.Id, isRandom: true);
        
        return randomBucket;

    }

    private void RegisterAvailableBuckets(IList<BucketModel> availableBuckets)
    {
        foreach (var availableBucket in availableBuckets)
        {
            if (!bucketSelectionStats.ContainsKey(availableBucket.Id))
            {
                bucketSelectionStats.Add(availableBucket.Id, new BucketSelectionStats
                {
                    SelectionCount = 0,
                    LastSelectionTime = DateTime.MinValue,
                });
            }
        }
    }

    // Cleanup policy for historical stats:
    // - We do NOT drop stats just because a bucket is missing from 'availableBuckets' — availability can fluctuate
    //   based on priority and other filters.
    // - To control memory growth without losing buckets that are still in rotation, we only remove stats when the
    //   bucket is currently unavailable AND it hasn't been selected for at least 24 hours.
    // - The 24h retention window is a conservative default that tolerates diurnal patterns; tune if real usage suggests otherwise.
    private void CleanUpBucketSelectionStats(IList<BucketModel> availableBuckets)
    {
        bucketSelectionStats
            .Where(x => !availableBuckets.Any(y => y.Id == x.Key))
            .Where(x => DateTime.UtcNow - x.Value.LastSelectionTime > TimeSpan.FromHours(24)) 
            .ToList()
            .ForEach(x => bucketSelectionStats.Remove(x.Key));
    }

    private void IncrementBucketSelectionStats(string bucketId, bool isRandom = false)
    {
        if (!bucketSelectionStats.TryGetValue(bucketId, out BucketSelectionStats? selectionStats))
        {
            selectionStats = new BucketSelectionStats
            {
                SelectionCount = 0,
                LastSelectionTime = DateTime.UtcNow,
            };
            
            bucketSelectionStats.Add(bucketId, selectionStats);
        }
        
        var stats = selectionStats;
        stats.SelectionCount++;
        stats.LastSelectionTime = DateTime.UtcNow;
        
        lastSelectedBucketId = bucketId;
        lastSelectionTime = DateTime.UtcNow;

        if (!isRandom)
        {
            picksSinceRandom++;
        }
    }
    
    private class BucketSelectionStats
    {
        public int SelectionCount { get; set; }
        public DateTime LastSelectionTime { get; set; }
    }
}