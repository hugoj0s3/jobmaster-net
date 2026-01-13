using System.Data;
using Dapper;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Ioc;
using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sql.Connections;
using JobMaster.Sql.Scripts;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Sql;

public abstract class SqlJobMasterRuntimeSetup : IJobMasterRuntimeSetup
{
    
    public virtual Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider)
    {
        return Task.FromResult<IList<string>>(new List<string>());
    }
    
    protected abstract int DefaultDbOperationThrottleLimitForCluster { get; }
    protected abstract int DefaultDbOperationThrottleLimitForAgent { get; }
    

    public virtual async Task OnStartingAsync(IServiceProvider mainServiceProvider)
    {
        Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;
        await ConfigClustersAsync();
        await ConfigAgentsAsync();
    }

    private async Task ConfigClustersAsync()
    {
        var configs = JobMasterClusterConnectionConfig.GetAllConfigs().Where(cfg => cfg.RepositoryTypeId == RepositoryTypeId).ToList();

        // Set default options
        foreach (var clusterConfig in configs)
        {
            if (!clusterConfig.RuntimeDbOperationThrottleLimit.HasValue)
            {
                clusterConfig.SetRuntimeDbOperationThrottleLimit(DefaultDbOperationThrottleLimitForCluster);
            }
            
            var clusterTablePrefix = 
                clusterConfig.AdditionalConnConfig.TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey);

            if (clusterTablePrefix == null)
            {
                clusterConfig.AdditionalConnConfig.SetValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, "JM_");
            }
        }
        
        foreach (var clusterConfig in configs)
        {
            if (!clusterConfig.IsAutoProvisionSqlSchemaEnabled())
            {
                return;
            }
            
            var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterConfig.ClusterId);
            var connManager = factory.ClusterServiceProvider.GetRequiredKeyedService<IDbConnectionManager>(this.RepositoryTypeId);
            
            using var conn = await connManager.OpenAsync(clusterConfig.ConnectionString, clusterConfig.AdditionalConnConfig);
            using var transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
            
            var sql = SqlGeneratorFactory.Get(RepositoryTypeId);
            var tablePrefix = sql.GetTablePrefix(clusterConfig.AdditionalConnConfig);
            
            var genericRecordTableExistsSql = sql.TableExistsSql(tablePrefix, sql.TableNameFor<GenericRecordEntry>());
            var genericRecordTableExists = await conn.QueryFirstOrDefaultAsync<bool>(genericRecordTableExistsSql, transaction: transaction);
            if (!genericRecordTableExists)
            {
                var genericRecordTablesScript = MasterTableCreatorScripts.CreateGenericRecordTablesScript(sql, tablePrefix);
                await conn.ExecuteAsync(genericRecordTablesScript, transaction: transaction);
            }
            
            var distributedLockerTableExistsSql = sql.TableExistsSql(tablePrefix, "distributed_lock");
            var distributedLockerTableExists = await conn.QueryFirstOrDefaultAsync<bool>(distributedLockerTableExistsSql, transaction: transaction);
            if (!distributedLockerTableExists)
            {
                var distributedLockerTableScript = MasterTableCreatorScripts.CreateDistributedLockTablesScript(sql, tablePrefix);
                await conn.ExecuteAsync(distributedLockerTableScript, transaction: transaction);
            }
            
            var jobTableExistsSql = sql.TableExistsSql(tablePrefix, sql.TableNameFor<Job>());
            var jobTableExists = await conn.QueryFirstOrDefaultAsync<bool>(jobTableExistsSql, transaction: transaction);
            if (!jobTableExists)
            {
                var jobTableScript = MasterTableCreatorScripts.CreateJobTablesScript(sql, tablePrefix);
                await conn.ExecuteAsync(jobTableScript, transaction: transaction);
            }
            
            var recurringScheduleTableExistsSql = sql.TableExistsSql(tablePrefix, sql.TableNameFor<RecurringSchedule>());
            var recurringScheduleTableExists = await conn.QueryFirstOrDefaultAsync<bool>(recurringScheduleTableExistsSql, transaction: transaction);
            if (!recurringScheduleTableExists)
            {
                var recurringScheduleTableScript = MasterTableCreatorScripts.CreateRecurringScheduleTablesScript(sql, tablePrefix);
                await conn.ExecuteAsync(recurringScheduleTableScript, transaction: transaction);
            }
            
            transaction.Commit();
        }
    }

    private async Task ConfigAgentsAsync()
    {
        var agentConfigs = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .SelectMany(x => x.GetAllAgentConnectionConfigs())
            .Where(a => a.RepositoryTypeId == RepositoryTypeId)
            .ToList();

        foreach (var agentConfig in agentConfigs)
        {
            var agentTablePrefix =
                agentConfig.AdditionalConnConfig.TryGetValue<string>(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey);

            if (agentTablePrefix == null)
            {
                agentConfig.AdditionalConnConfig.SetValue(SqlBaseConfigKeys.NamespaceUniqueKey, SqlBaseConfigKeys.TablePrefixKey, "JM_");
            }
            
            if (!agentConfig.RuntimeDbOperationThrottleLimit.HasValue)
            {
                agentConfig.SetRuntimeDbOperationThrottleLimit(this.DefaultDbOperationThrottleLimitForAgent);
            }
        }

        foreach (var agentConfig in agentConfigs)
        {
            var clusterConfig = JobMasterClusterConnectionConfig.Get(agentConfig.ClusterId, includeInactive: true);
            if (!clusterConfig.IsAutoProvisionSqlSchemaEnabled())
            {
                return;
            }

            var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterConfig.ClusterId);
            var connManager = factory.ClusterServiceProvider.GetRequiredKeyedService<IDbConnectionManager>(agentConfig.RepositoryTypeId);
            
            using var agentDbConnection = await connManager.OpenAsync(agentConfig.ConnectionString, agentConfig.AdditionalConnConfig);
            using var agentTransaction = agentDbConnection.BeginTransaction(IsolationLevel.ReadCommitted);
                
            var agentSql = SqlGeneratorFactory.Get(agentConfig.RepositoryTypeId);
            var agentTablePrefix = agentSql.GetTablePrefix(agentConfig.AdditionalConnConfig);
                
            var bucketSelectorTableExistsSql = agentSql.TableExistsSql(agentTablePrefix, "bucket_dispatcher");
            var bucketSelectorTableExists = await agentDbConnection.QueryFirstOrDefaultAsync<bool>(bucketSelectorTableExistsSql, transaction: agentTransaction);
            if (!bucketSelectorTableExists)
            {
                var bucketSelectorTableScript = AgentTableCreatorScripts.CreateBucketDispatcherTableScript(agentSql, agentTablePrefix);
                await agentDbConnection.ExecuteAsync(bucketSelectorTableScript, transaction: agentTransaction);
            }
                
            var messageTableExistsSql = agentSql.TableExistsSql(agentTablePrefix, "message_dispatcher");
            var messageTableExists = await agentDbConnection.QueryFirstOrDefaultAsync<bool>(messageTableExistsSql, transaction: agentTransaction);
            if (!messageTableExists)
            {
                var messageTableScript = AgentTableCreatorScripts.CreateMessageDispatcherTableScript(agentSql, agentTablePrefix);
                await agentDbConnection.ExecuteAsync(messageTableScript, transaction: agentTransaction);
            }
                
            agentTransaction.Commit();
        }
    }

    public abstract string RepositoryTypeId { get; }
}