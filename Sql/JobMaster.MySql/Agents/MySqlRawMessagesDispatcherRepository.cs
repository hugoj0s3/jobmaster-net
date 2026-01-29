using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.SqlBase.Agents;
using JobMaster.SqlBase.Connections;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.MySql.Agents;

internal class MySqlRawMessagesDispatcherRepository : SqlRawMessagesDispatcherRepositoryBase
{
    public MySqlRawMessagesDispatcherRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IServiceProvider serviceProvider,
        IJobMasterLogger logger) : base(
            clusterConnectionConfig,
            serviceProvider.GetRequiredKeyedService<IDbConnectionManager>(MySqlRepositoryConstants.RepositoryTypeId),
            logger)
    {
    }

    public override string AgentRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;
    
    protected override (string Table, string ColMsgId, string ColBucket, string SelectSql) GetDequeueSelectSql(
        int numberOfJobs, 
        bool referenceTimeToHasValue)
    {
        var baseResult = base.GetDequeueSelectSql(numberOfJobs, referenceTimeToHasValue);
        var lockedSql = baseResult.SelectSql + " FOR UPDATE SKIP LOCKED";
        return (baseResult.Table, baseResult.ColMsgId, baseResult.ColBucket, lockedSql);
    }
    
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
