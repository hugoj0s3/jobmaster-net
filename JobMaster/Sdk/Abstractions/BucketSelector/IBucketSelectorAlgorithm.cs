using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Sdk.Abstractions.BucketSelector;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IBucketSelectorAlgorithm
{
    BucketModel? Select(IList<BucketModel> availableBuckets);
}

