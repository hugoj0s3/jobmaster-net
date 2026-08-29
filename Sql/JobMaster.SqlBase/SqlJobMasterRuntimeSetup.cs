using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Scripts;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.SqlBase;

/// <summary>
/// Abstract base class for SQL-provider runtime setup. Handles automatic schema provisioning
/// (tables for jobs, recurring schedules, generic records, distributed locks, and agent buckets)
/// on first startup, and sets provider-specific defaults such as DB operation throttle limits
/// and table prefixes.
/// </summary>
public abstract class SqlJobMasterRuntimeSetup : IJobMasterRuntimeSetup
{
    /// <summary>
    /// Validates the SQL configuration before startup. Override to add provider-specific checks.
    /// Returns a list of validation error messages; an empty list means validation passed.
    /// </summary>
    public virtual Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider)
    {
        return Task.FromResult<IList<string>>(new List<string>());
    }
    
    /// <summary>
    /// Applies provider defaults (table prefix, throttle limits) and provisions the schema on first startup
    /// if auto-provisioning is enabled.
    /// </summary>
    public virtual async Task OnBeforeStartAsync(IServiceProvider mainServiceProvider)
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
            
            // Provision generic record family tables (default + all family suffixes)
            var allEntryTableNames = MasterTableCreatorScripts.AllGenericRecordTableNames(sql, tablePrefix);
            foreach (var entryTableName in allEntryTableNames)
            {
                var tableExistsSql = sql.TableExistsSql(string.Empty, entryTableName);
                var tableExists = await conn.QueryFirstOrDefaultAsync<bool>(tableExistsSql, transaction: transaction);
                if (!tableExists)
                {
                    var genericRecordTablesScript = MasterTableCreatorScripts.CreateGenericRecordTablesScript(sql, tablePrefix);
                    await conn.ExecuteAsync(genericRecordTablesScript, transaction: transaction);
                    break; // All tables created at once, no need to check the rest
                }
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

            var jobExecutionTableExistsSql = sql.TableExistsSql(tablePrefix, "job_execution");
            var jobExecutionTableExists = await conn.QueryFirstOrDefaultAsync<bool>(jobExecutionTableExistsSql, transaction: transaction);
            if (!jobExecutionTableExists)
            {
                var jobExecutionTableScript = MasterTableCreatorScripts.CreateJobExecutionTableScript(sql, tablePrefix);
                await conn.ExecuteAsync(jobExecutionTableScript, transaction: transaction);
            }

            var logTableExistsSql = sql.TableExistsSql(tablePrefix, "log");
            var logTableExists = await conn.QueryFirstOrDefaultAsync<bool>(logTableExistsSql, transaction: transaction);
            if (!logTableExists)
            {
                var logTableScript = MasterTableCreatorScripts.CreateLogTableScript(sql, tablePrefix);
                await conn.ExecuteAsync(logTableScript, transaction: transaction);
            }

            // JobMasterRuntime.StartAsync lazily registers a "master fallback" agent connection
            // for any cluster with a Full/Coordinator worker (AddMasterFallbackAgentConnectionString),
            // reusing this exact connection string and AdditionalConnConfig — but only *after* this
            // provisioning step has already run, so that connection's own agent-shaped tables would
            // never get created otherwise. Since it's guaranteed to point at this same database with
            // this same table prefix, provision them here too, so they already exist by the time the
            // fallback connection is used.
            await ProvisionAgentTablesAsync(conn, transaction, sql, tablePrefix);

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
        }

        foreach (var agentConfig in agentConfigs)
        {
            var clusterConfig = JobMasterClusterConnectionConfig.Get(agentConfig.ClusterId, includeNotReady: true);
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

            await ProvisionAgentTablesAsync(agentDbConnection, agentTransaction, agentSql, agentTablePrefix);

            agentTransaction.Commit();
        }
    }

    /// <summary>
    /// Creates the three agent-shaped tables (bucket_dispatcher, message_dispatcher,
    /// agent_conn_fingerprint) on <paramref name="conn"/>/<paramref name="tablePrefix"/> if they
    /// don't already exist. Shared by ConfigAgentsAsync (real agent connections) and
    /// ConfigClustersAsync (the master DB, which also needs these — see the call site there for why).
    /// </summary>
    private static async Task ProvisionAgentTablesAsync(IDbConnection conn, IDbTransaction transaction, ISqlGenerator sql, string tablePrefix)
    {
        var bucketDispatcherTableExistsSql = sql.TableExistsSql(tablePrefix, "bucket_dispatcher");
        var bucketDispatcherTableExists = await conn.QueryFirstOrDefaultAsync<bool>(bucketDispatcherTableExistsSql, transaction: transaction);
        if (!bucketDispatcherTableExists)
        {
            var bucketDispatcherTableScript = AgentTableCreatorScripts.CreateBucketDispatcherTableScript(sql, tablePrefix);
            await conn.ExecuteAsync(bucketDispatcherTableScript, transaction: transaction);
        }

        var messageDispatcherTableExistsSql = sql.TableExistsSql(tablePrefix, "message_dispatcher");
        var messageDispatcherTableExists = await conn.QueryFirstOrDefaultAsync<bool>(messageDispatcherTableExistsSql, transaction: transaction);
        if (!messageDispatcherTableExists)
        {
            var messageDispatcherTableScript = AgentTableCreatorScripts.CreateMessageDispatcherTableScript(sql, tablePrefix);
            await conn.ExecuteAsync(messageDispatcherTableScript, transaction: transaction);
        }

        var fingerprintTableExistsSql = sql.TableExistsSql(tablePrefix, "agent_conn_fingerprint");
        var fingerprintTableExists = await conn.QueryFirstOrDefaultAsync<bool>(fingerprintTableExistsSql, transaction: transaction);
        if (!fingerprintTableExists)
        {
            var fingerprintTableScript = AgentTableCreatorScripts.CreateAgentConnectionFingerprint(sql, tablePrefix);
            await conn.ExecuteAsync(fingerprintTableScript, transaction: transaction);
        }
    }

    /// <summary>The repository type identifier used to match SQL configurations and keyed services.</summary>
    public abstract string RepositoryTypeId { get; }
}