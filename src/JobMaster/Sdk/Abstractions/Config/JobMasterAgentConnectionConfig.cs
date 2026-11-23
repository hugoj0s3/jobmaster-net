using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Config;

internal class JobMasterAgentConnectionConfig
{
    public static JobMasterAgentConnectionConfig NewSingle(
        string clusterId,
        string name,
        string connectionString,
        string repositoryTypeId,
        JobMasterConfigDictionary? additionalConnConfig = null)
    {
        var providerConfig = new JobMasterAgentProviderConfig(repositoryTypeId, connectionString, additionalConnConfig);
        return new JobMasterAgentConnectionConfig(clusterId, name, false, repositoryTypeId, providerConfig, providerConfig);
    }

    public static JobMasterAgentConnectionConfig NewHybrid(
        string clusterId,
        string name,
        JobMasterAgentProviderConfig transportConfig,
        JobMasterAgentProviderConfig executionConfig)
    {
        if (transportConfig is null)
            throw new ArgumentNullException(nameof(transportConfig));
        if (executionConfig is null)
            throw new ArgumentNullException(nameof(executionConfig));
        if (ReferenceEquals(transportConfig, executionConfig))
            throw new ArgumentException("Transport and execution configs must be different instances for a hybrid connection.");
        
        var repositoryTypeId = RepoTypeAndFingerprintUtil.FormatHybridRepoType(
            transportConfig.RepositoryTypeId,
            executionConfig.RepositoryTypeId);
        
        return new JobMasterAgentConnectionConfig(clusterId, name, true, repositoryTypeId, transportConfig, executionConfig);
    }
    
    private JobMasterAgentConnectionConfig(
        string clusterId,
        string name,
        bool isHybrid,
        string repositoryTypeId,
        JobMasterAgentProviderConfig transportConfig,
        JobMasterAgentProviderConfig executionConfig)
    {
        TransportConfig = transportConfig ?? throw new ArgumentNullException(nameof(transportConfig));
        ExecutionConfig = executionConfig ?? throw new ArgumentNullException(nameof(executionConfig));
        
        ClusterId = clusterId;
        Name = name;
        Id = new AgentConnectionId(clusterId, name).IdValue;
        RepositoryTypeId = repositoryTypeId;
        IsHybrid = isHybrid;
    }

    public string Id { get; private set; }
    public string ClusterId { get; private set; }
    public string Name { get; private set; }
    public string RepositoryTypeId { get; private set; }
    public bool IsHybrid { get; private set; }

    /// <summary>Same instance as <see cref="ExecutionConfig"/> for a non-hybrid connection.</summary>
    public JobMasterAgentProviderConfig TransportConfig { get; private set; }
    /// <summary>Same instance as <see cref="TransportConfig"/> for a non-hybrid connection.</summary>
    public JobMasterAgentProviderConfig ExecutionConfig { get; private set; }
}
