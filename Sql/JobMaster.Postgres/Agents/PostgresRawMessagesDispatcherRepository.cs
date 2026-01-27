using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.SqlBase.Agents;
using JobMaster.SqlBase.Connections;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Postgres.Agents;

internal class PostgresRawMessagesDispatcherRepository : SqlRawMessagesDispatcherRepositoryBase
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