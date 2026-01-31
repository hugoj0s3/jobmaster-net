using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;

namespace JobMaster.Sdk.Abstractions.Services.Master;

internal interface IMasterBucketsService : IJobMasterClusterAwareService
{
    Task DestroyAsync(string bucketId);
    Task<BucketModel> CreateAsync(AgentConnectionId agentConnectionId, string workerId, JobMasterPriority priority);
    void Update(BucketModel model);
    Task UpdateAsync(BucketModel model);
    BucketModel? SelectBucket(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null);
    Task<BucketModel?> SelectBucketAsync(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null);
    BucketModel? Get(string bucketId, TimeSpan? allowedDiscrepancy);
    Task<IList<BucketModel>> QueryAllNoCacheAsync(BucketStatus? bucketStatus = null);
    IList<BucketModel> QueryAllNoCache(BucketStatus? bucketStatus = null);
    
    Task<IList<BucketModel>> QueryAsync(MasterBucketQueryCriteria criteria);
    IList<BucketModel> Query(MasterBucketQueryCriteria criteria);
    
    Task<int> CountAsync(MasterBucketQueryCriteria criteria);
    int Count(MasterBucketQueryCriteria criteria);
}
