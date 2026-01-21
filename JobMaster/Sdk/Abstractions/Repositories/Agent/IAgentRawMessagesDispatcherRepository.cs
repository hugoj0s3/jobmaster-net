using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IAgentRawMessagesDispatcherRepository : IJobMasterClusterAwareComponent
{
    string PushMessage(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId);
    Task<string> PushMessageAsync(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId);
    Task<IList<string>> BulkPushMessageAsync(string fullBucketAddressId, IList<(string payload, DateTime referenceTime, string correlationId)> messages);
    Task<IList<JobMasterRawMessage>> DequeueMessagesAsync(string fullBucketAddressId, int numberOfJobs, DateTime? referenceTimeTo = null);
    Task<bool> HasJobsAsync(string fullBucketAddressId);
    
    Task CreateBucketAsync(string fullBucketAddressId);
    Task DestroyBucketAsync(string fullBucketAddressId);
    
    void Initialize(JobMasterAgentConnectionConfig config);
    
    bool IsAutoDequeue { get; }
    
    string AgentRepoTypeId { get; }
}