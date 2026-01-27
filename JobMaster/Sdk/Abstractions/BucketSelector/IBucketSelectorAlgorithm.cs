using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Sdk.Abstractions.BucketSelector;

internal interface IBucketSelectorAlgorithm
{
    BucketModel? Select(IList<BucketModel> availableBuckets);
}

