using System;
using System.Threading.Tasks;
using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models.Agents;
using JobMaster.Sdk.Contracts.Models.Buckets;

namespace JobMaster.Sdk.Contracts.Services.Master;

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
