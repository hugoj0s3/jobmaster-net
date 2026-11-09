using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Config;

internal class JobMasterAgentProviderConfig : IHasAdditionalConnConfig
{
    public JobMasterAgentProviderConfig(
        string repositoryTypeId,
        string connectionString,
        JobMasterConfigDictionary? additionalConnConfig = null)
    {
        RepositoryTypeId = repositoryTypeId;
        ConnectionString = connectionString;
        AdditionalConnConfig = additionalConnConfig ?? new JobMasterConfigDictionary();
    }

    public string RepositoryTypeId { get; private set; }
    public string ConnectionString { get; private set; }
    public JobMasterConfigDictionary AdditionalConnConfig { get; private set; }
}

internal class JobMasterAgentConnectionConfig
{
    public JobMasterAgentConnectionConfig(
        string clusterId,
        string name,
        string connectionString,
        string repositoryTypeId,
        JobMasterConfigDictionary? additionalConnConfig = null)
    {
        ClusterId = clusterId;
        Name = name;
        Id = new AgentConnectionId(clusterId, name).IdValue;
        RepositoryTypeId = repositoryTypeId;
        IsHybrid = false;
        // One store serves both roles.
        var providerConfig = new JobMasterAgentProviderConfig(repositoryTypeId, connectionString, additionalConnConfig);
        TransportConfig = providerConfig;
        ExecutionConfig = providerConfig;
    }

    public JobMasterAgentConnectionConfig(
        string clusterId,
        string name,
        JobMasterAgentProviderConfig transportConfig,
        JobMasterAgentProviderConfig executionConfig)
    {
        TransportConfig = transportConfig ?? throw new ArgumentNullException(nameof(transportConfig));
        ExecutionConfig = executionConfig ?? throw new ArgumentNullException(nameof(executionConfig));
        ClusterId = clusterId;
        Name = name;
        Id = new AgentConnectionId(clusterId, name).IdValue;
        RepositoryTypeId = RepoTypeAndFingerprintUtil.FormatHybridRepoType(
            transportConfig.RepositoryTypeId,
            executionConfig.RepositoryTypeId);
        IsHybrid = true;
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
