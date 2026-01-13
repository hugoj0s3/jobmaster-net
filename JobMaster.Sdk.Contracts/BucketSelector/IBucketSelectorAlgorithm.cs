using JobMaster.Sdk.Contracts.Models.Buckets;

namespace JobMaster.Sdk.Contracts.BucketSelector;

public interface IBucketSelectorAlgorithm
{
    BucketModel? Select(IList<BucketModel> availableBuckets);
}

