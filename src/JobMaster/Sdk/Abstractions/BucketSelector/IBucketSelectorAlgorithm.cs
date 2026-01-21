using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Sdk.Abstractions.BucketSelector;

public interface IBucketSelectorAlgorithm
{
    BucketModel? Select(IList<BucketModel> availableBuckets);
}

