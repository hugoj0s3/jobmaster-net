using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sql.Agents;
using JobMaster.Sql.Connections;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.SqlServer.Agents;

public class SqlServerRawMessagesDispatcherRepository : SqlRawMessagesDispatcherRepositoryBase
{
    public SqlServerRawMessagesDispatcherRepository(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IServiceProvider serviceProvider,
        IJobMasterLogger logger) : base(
            clusterConnConfig,
            serviceProvider.GetRequiredKeyedService<IDbConnectionManager>(SqlServerRepositoryConstants.RepositoryTypeId),
            logger)
    {
    }

    public override string AgentRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
    
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
