using System.Data;
using JobMaster.Sql.Agents;
using JobMaster.Sql.Connections;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Services.Master;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Postgres.Agents;

public class PostgresRawMessagesDispatcherRepository : SqlRawMessagesDispatcherRepositoryBase
{
    public PostgresRawMessagesDispatcherRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IServiceProvider serviceProvider,
        IJobMasterLogger logger) : base(
            clusterConnectionConfig,
            serviceProvider.GetRequiredKeyedService<IDbConnectionManager>(PostgresRepositoryConstants.RepositoryTypeId),
            logger)
    {
    }

    public override string AgentRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;
    
    public override async Task<IList<JobMasterRawMessage>> DequeueMessagesAsync(string fullBucketAddressId, int numberOfJobs, DateTime? referenceTimeTo = null)
    {
        using var cnn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = cnn.BeginTransaction(IsolationLevel.ReadCommitted);
        
        try
        {
            var result = await DequeueMessagesAsyncCore(cnn, tx, fullBucketAddressId, numberOfJobs, referenceTimeTo);
            tx.Commit();
            return result;
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }
}