using JobMaster.Contracts.Extensions;
using JobMaster.Sdk.Contracts.BucketSelector;
using JobMaster.Sdk.Contracts.Models.Buckets;

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