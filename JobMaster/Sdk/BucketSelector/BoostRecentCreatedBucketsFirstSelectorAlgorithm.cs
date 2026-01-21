using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.BucketSelector;
using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Sdk.BucketSelector;

[Obsolete]
internal class BoostRecentCreatedBucketsFirstSelectorAlgorithm : IBucketSelectorAlgorithm
{
    private readonly TimeSpan threshold;
    private readonly IBucketSelectorAlgorithm bucketSelectorAlgorithm;
    public BoostRecentCreatedBucketsFirstSelectorAlgorithm(TimeSpan threshold, IBucketSelectorAlgorithm bucketSelectorAlgorithm)
    {
        this.threshold = threshold;
        this.bucketSelectorAlgorithm = bucketSelectorAlgorithm;
    }

    public BucketModel? Select(IList<BucketModel> availableBuckets)
    {
        if (availableBuckets.IsNullOrEmpty())
        {
            return null;
        }

        // Filter recent buckets based on the threshold
        var recentCreatedBuckets = availableBuckets
            .Where(x => x.CreatedAt > DateTime.UtcNow.Subtract(threshold))
            .ToList();

        var bucketToSelect = recentCreatedBuckets.Count > 0 ? recentCreatedBuckets : availableBuckets;

        return bucketSelectorAlgorithm.Select(bucketToSelect);
    }
}