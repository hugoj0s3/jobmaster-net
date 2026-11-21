using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

internal interface IAgentRawMessagesDispatcherRepository : IJobMasterClusterAwareComponent
{
    string PushMessage(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId);
    Task<string> PushMessageAsync(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId);
    Task<IList<string>> BulkPushMessageAsync(string fullBucketAddressId, IList<(string payload, DateTime referenceTime, string correlationId)> messages);
    Task<IList<JobMasterRawMessage>> PullMessagesAsync(string fullBucketAddressId, int numberOfJobs, DateTime? referenceTimeTo = null);

    /// <summary>
    /// Like <see cref="PullMessagesAsync"/>, but the messages stay stored, hidden from other pulls for
    /// <paramref name="lease"/>; one not acked (<see cref="AckAsync"/>) by then can be pulled again. Lease + ack is
    /// the at-least-once read behind <see cref="IAgentJobsDispatcherRepository.LeaseSavePendingJobsAsync"/>.
    /// </summary>
    Task<IList<JobMasterRawMessage>> LeaseMessagesAsync(string fullBucketAddressId, int numberOfJobs, TimeSpan lease);

    /// <summary>
    /// Confirms leased messages are handled, removing them: the second half of <see cref="LeaseMessagesAsync"/>.
    /// Ids already gone (removed before, or never stored) are ignored.
    /// </summary>
    Task AckAsync(string fullBucketAddressId, IList<string> messageIds);

    /// <summary>
    /// Moves messages of <paramref name="fullBucketAddressId"/> to <paramref name="newFullBucketAddressId"/> (which
    /// must exist), unleased. Ids not in the bucket are ignored.
    /// </summary>
    Task UpdateBucketAsync(string fullBucketAddressId, IList<string> messageIds, string newFullBucketAddressId);
    Task<bool> HasJobsAsync(string fullBucketAddressId);

    Task CreateBucketAsync(string fullBucketAddressId);
    Task CreateBucketIfNotExistsAsync(string fullBucketAddressId);
    Task DestroyBucketAsync(string fullBucketAddressId);
    
    /// <remarks>Must be called exactly once; a second call throws.</remarks>
    void Initialize(string agentConnectionId, JobMasterAgentProviderConfig config);

    bool IsInitialized { get; }

    bool IsPollingBased { get; }
    
    string AgentRepoTypeId { get; }
}
