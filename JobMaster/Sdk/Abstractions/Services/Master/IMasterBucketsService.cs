using System.ComponentModel;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Sdk.Abstractions.Services.Master;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IMasterBucketsService : IJobMasterClusterAwareService
{
    Task DestroyAsync(string bucketId);
    Task<BucketModel> CreateAsync(AgentConnectionId agentConnectionId, string workerId, JobMasterPriority priority);
    void Update(BucketModel model);
    Task UpdateAsync(BucketModel model);
    BucketModel? SelectBucket(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null);
    Task<BucketModel?> SelectBucketAsync(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null);
    BucketModel? Get(string bucketId, TimeSpan? allowedDiscrepancy);
    Task<IList<BucketModel>> QueryAllNoCacheAsync(BucketStatus? bucketStatus = null);
    List<BucketModel> QueryAllNoCache(BucketStatus? bucketStatus = null);
}
