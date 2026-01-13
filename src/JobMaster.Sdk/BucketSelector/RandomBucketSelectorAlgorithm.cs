using JobMaster.Contracts.Extensions;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Contracts.BucketSelector;
using JobMaster.Sdk.Contracts.Models.Buckets;

namespace JobMaster.Sdk.BucketSelector;

public class RandomBucketSelectorAlgorithm : IBucketSelectorAlgorithm
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