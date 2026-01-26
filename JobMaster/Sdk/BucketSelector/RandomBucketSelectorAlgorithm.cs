using JobMaster.Sdk.Abstractions.BucketSelector;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.BucketSelector;

internal class RandomBucketSelectorAlgorithm : IBucketSelectorAlgorithm
{
    public BucketModel? Select(IList<BucketModel> availableBuckets)
    {
        // 1. Safety Checks
        if (availableBuckets.Count == 0)
        {
            return null;
        }

        // 2. Optimization: Don't calculate random if there's only one choice
        if (availableBuckets.Count == 1)
        {
            return availableBuckets[0];
        }

        return availableBuckets.Random();
    }
}