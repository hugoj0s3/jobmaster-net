using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Postgres;

internal class PostgresJobMasterRuntimeSetup : SqlJobMasterRuntimeSetup
{
    protected override int DefaultDbOperationThrottleLimitForCluster => 250;
    protected override int DefaultDbOperationThrottleLimitForAgent => 50;
    public override string RepositoryTypeId => PostgresRepositoryConstants.RepositoryTypeId;

    public override async Task OnStartingAsync(IServiceProvider mainServiceProvider)
    {
        await CreateCaseInsensitiveCollationAsync();
        await base.OnStartingAsync(mainServiceProvider);
    }

    private async Task CreateCaseInsensitiveCollationAsync()
    {
        var configs = JobMasterClusterConnectionConfig.GetAllConfigs()
            .Where(cfg => cfg.RepositoryTypeId == RepositoryTypeId)
            .ToList();

        foreach (var clusterConfig in configs)
        {
            if (!clusterConfig.IsAutoProvisionSqlSchemaEnabled())
            {
                continue;
            }

            var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterConfig.ClusterId);
            var connManager = factory.ClusterServiceProvider.GetRequiredKeyedService<IDbConnectionManager>(this.RepositoryTypeId);

            using var conn = await connManager.OpenAsync(clusterConfig.ConnectionString, clusterConfig.AdditionalConnConfig);
            await conn.ExecuteAsync(PostgresRepositoryConstants.CreateCaseInsensitiveCollationSql);
        }
        
        var agentConfigs = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .SelectMany(x => x.GetAllAgentConnectionConfigs())
            .Where(a => a.RepositoryTypeId == RepositoryTypeId)
            .ToList();

        foreach (var config in agentConfigs)
        {
            var clusterConfig = JobMasterClusterConnectionConfig
                .Get(config.ClusterId, includeNotReady:true);
            if (!clusterConfig.IsAutoProvisionSqlSchemaEnabled())
            {
                continue;
            }
            
            var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterConfig.ClusterId);
            var connManager = factory.ClusterServiceProvider.GetRequiredKeyedService<IDbConnectionManager>(config.RepositoryTypeId);

            using var conn = await connManager.OpenAsync(config.ConnectionString, config.AdditionalConnConfig);
            await conn.ExecuteAsync(PostgresRepositoryConstants.CreateCaseInsensitiveCollationSql);
        }
    }
}