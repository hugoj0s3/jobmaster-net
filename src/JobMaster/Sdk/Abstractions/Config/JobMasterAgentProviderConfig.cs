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
