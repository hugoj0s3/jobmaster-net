using System.Data;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sql.Agents;
using JobMaster.Sql.Connections;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Services.Master;
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
