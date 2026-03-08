using System.Data;
using Dapper;
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
    
    protected override async Task<IList<JobMasterRawMessage>> DequeueMessagesAsyncCore(
        IDbConnection cnn, 
        IDbTransaction tx, 
        string fullBucketAddressId, 
        int numberOfJobs, 
        DateTime? referenceTimeTo = null)
    {
        // Postgres-specific: DELETE ... RETURNING for atomic dequeue (single operation instead of SELECT + DELETE)
        var table = MessageTableName();
        var colMsgId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.MessageId);
        var colPayload = sql.ColumnNameFor<JobMasterRawMessage>(x => x.Payload);
        var colRefTime = sql.ColumnNameFor<JobMasterRawMessage>(x => x.ReferenceTime);
        var colCorrId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.CorrelationId);
        var colEnqAt = sql.ColumnNameFor<JobMasterRawMessage>(x => x.EnqueuedAt);
        var colBucket = "bucket_address_id";

        var deleteSql = $@"
DELETE FROM {table}
WHERE {colMsgId} IN (
    SELECT {colMsgId}
    FROM {table}
    WHERE {colBucket} = @Bucket";

        if (referenceTimeTo.HasValue)
        {
            deleteSql += $" AND {colRefTime} <= @RefTo";
        }

        deleteSql += $@"
    ORDER BY {colRefTime} ASC, {colMsgId} ASC
    LIMIT @Limit
    FOR UPDATE SKIP LOCKED
)
RETURNING {colMsgId}, {colPayload}, {colRefTime}, {colCorrId}, {colEnqAt};";

        var rows = await cnn.QueryAsync<JobMasterRawMessagePersistenceRecord>(deleteSql, new
        {
            Bucket = fullBucketAddressId,
            RefTo = (referenceTimeTo ?? DateTime.UtcNow).ToUniversalTime(),
            Limit = numberOfJobs
        }, tx);

        return rows.Select(r => JobMasterRawMessage.RecoverFromDb(r)).ToList();
    }
}