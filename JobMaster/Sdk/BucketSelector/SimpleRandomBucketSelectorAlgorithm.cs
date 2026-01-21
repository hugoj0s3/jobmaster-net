using JobMaster.Internals;
using JobMaster.Sdk.Abstractions.BucketSelector;
using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Sdk.BucketSelector;

[Obsolete]
internal class SimpleRandomBucketSelectorAlgorithm : IBucketSelectorAlgorithm
{
    public BucketModel? Select(IList<BucketModel> availableBuckets)
    {
        if (availableBuckets.IsNullOrEmpty())
        {
            return null;
        }
        
        // Randomly select a bucket.
        return availableBuckets.Random();
    }
}