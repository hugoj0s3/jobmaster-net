using JobMaster.Sdk.Contracts.Ioc;
using JobMaster.Sdk.Contracts.Repositories.Agent;

namespace JobMaster.Sdk.Ioc;

public static class ClusterServiceKeys
{
    // Key prefixes
    private const string ClusterServiceProviderPrefix = nameof(IJobMasterClusterAwareComponentFactory) + ":";
    private const string AgentJobsDispatcherPrefix = nameof(IAgentJobsDispatcherRepository) + ":";
    private const string AgentRawJobsDispatcherSavingPrefix = nameof(IAgentRawMessagesDispatcherRepository) + ":SavingPending:";
    private const string AgentRawJobsDispatcherProcessingPrefix = nameof(IAgentRawMessagesDispatcherRepository) + ":Processing:";

    // Key generation methods
    public static string GetClusterServiceProviderKey(string clusterId) 
        => $"{ClusterServiceProviderPrefix}{clusterId}";

    public static string GetAgentJobsDispatcherKey(string repositoryTypeId) 
        => $"{AgentJobsDispatcherPrefix}{repositoryTypeId}";

    public static string GetAgentRawJobsDispatcherSavingKey(string repositoryTypeId) 
        => $"{AgentRawJobsDispatcherSavingPrefix}{repositoryTypeId}";

    public static string GetAgentRawJobsDispatcherProcessingKey(string repositoryTypeId) 
        => $"{AgentRawJobsDispatcherProcessingPrefix}{repositoryTypeId}";
}