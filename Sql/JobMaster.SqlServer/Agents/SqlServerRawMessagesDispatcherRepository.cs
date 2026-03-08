using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.SqlBase.Agents;
using JobMaster.SqlBase.Connections;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.SqlServer.Agents;

internal class SqlServerRawMessagesDispatcherRepository : SqlRawMessagesDispatcherRepositoryBase
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

    protected override async Task<IList<JobMasterRawMessage>> DequeueMessagesAsyncCore(
        IDbConnection cnn, 
        IDbTransaction tx, 
        string fullBucketAddressId, 
        int numberOfJobs, 
        DateTime? referenceTimeTo = null)
    {
        // SQL Server-specific: DELETE with OUTPUT clause for atomic dequeue (single operation instead of SELECT + DELETE)
        var table = MessageTableName();
        var colMsgId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.MessageId);
        var colPayload = sql.ColumnNameFor<JobMasterRawMessage>(x => x.Payload);
        var colRefTime = sql.ColumnNameFor<JobMasterRawMessage>(x => x.ReferenceTime);
        var colCorrId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.CorrelationId);
        var colEnqAt = sql.ColumnNameFor<JobMasterRawMessage>(x => x.EnqueuedAt);
        var colBucket = "bucket_address_id";

        var deleteSql = $@"
DELETE TOP (@Limit) FROM {table}
OUTPUT DELETED.{colMsgId}, DELETED.{colPayload}, DELETED.{colRefTime}, DELETED.{colCorrId}, DELETED.{colEnqAt}
WHERE {colBucket} = @Bucket";

        if (referenceTimeTo.HasValue)
        {
            deleteSql += $" AND {colRefTime} <= @RefTo";
        }

        deleteSql += $@"
ORDER BY {colRefTime} ASC, {colMsgId} ASC;";

        var rows = await cnn.QueryAsync<JobMasterRawMessagePersistenceRecord>(deleteSql, new
        {
            Bucket = fullBucketAddressId,
            RefTo = (referenceTimeTo ?? DateTime.UtcNow).ToUniversalTime(),
            Limit = numberOfJobs
        }, tx);

        return rows.Select(r => JobMasterRawMessage.RecoverFromDb(r)).ToList();
    }
}
