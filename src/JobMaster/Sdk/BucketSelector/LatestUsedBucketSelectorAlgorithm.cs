using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.BucketSelector;
using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Sdk.BucketSelector;

[Obsolete]
internal class LatestUsedBucketSelectorAlgorithm : IBucketSelectorAlgorithm
{
    private readonly Dictionary<string, DateTime> latestSelection = new Dictionary<string, DateTime>();
    private readonly Dictionary<string, DateTime> bucketCreationDates = new Dictionary<string, DateTime>();

    public BucketModel? Select(IList<BucketModel> availableBuckets)
    {
        // Remove buckets that are older than 24 hours since creation and not selected in the last 2 hours
        var currentTime = DateTime.UtcNow;
        var bucketsToRemove = bucketCreationDates.Where(x => currentTime - x.Value > TimeSpan.FromHours(24))
            .Where(x => !latestSelection.ContainsKey(x.Key) || 
                        currentTime - latestSelection[x.Key] > TimeSpan.FromHours(2))
            .Select(x => x.Key)
            .ToList();
        foreach (var bucketId in bucketsToRemove)
        {
            bucketCreationDates.Remove(bucketId);
            latestSelection.Remove(bucketId);
        }

        // Find the bucket that was least recently selected
        availableBuckets
            .Where(b => !latestSelection.ContainsKey(b.Id))
            .ToList()
            .ForEach(x =>
            {
                latestSelection.Add(x.Id, DateTime.MinValue);
                bucketCreationDates.Add(x.Id, DateTime.UtcNow);
            });

        var leastRecentlySelectedBucket = availableBuckets.MinBy(x => latestSelection[x.Id]);

        if (leastRecentlySelectedBucket != null)
        {
            // Update the latest selection time for the selected bucket
            latestSelection[leastRecentlySelectedBucket.Id] = DateTime.UtcNow;
        }

        return leastRecentlySelectedBucket;
    }
}