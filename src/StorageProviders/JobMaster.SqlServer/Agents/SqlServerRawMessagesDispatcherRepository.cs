using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.SqlBase.Agents;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Scripts;
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
        // SQL Server-specific: Use CTE to select TOP (@Limit) IDs in order, then DELETE with JOIN and OUTPUT
        var table = MessageTableName();
        var colMsgId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.MessageId);
        var colPayload = sql.ColumnNameFor<JobMasterRawMessage>(x => x.Payload);
        var colRefTime = sql.ColumnNameFor<JobMasterRawMessage>(x => x.ReferenceTime);
        var colCorrId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.CorrelationId);
        var colEnqAt = sql.ColumnNameFor<JobMasterRawMessage>(x => x.EnqueuedAt);
        var colBucket = "bucket_address_id";

        var deleteSql = $@"
WITH cte AS (
    SELECT TOP (@Limit) {colMsgId}
    FROM {table}
    WHERE {colBucket} = @Bucket";

        if (referenceTimeTo.HasValue)
        {
            deleteSql += $" AND {colRefTime} <= @RefTo";
        }

        deleteSql += $@"
    ORDER BY {colRefTime} ASC, {colMsgId} ASC
)
DELETE t
OUTPUT DELETED.{colMsgId}, DELETED.{colPayload}, DELETED.{colRefTime}, DELETED.{colCorrId}, DELETED.{colEnqAt}
FROM {table} t
INNER JOIN cte ON t.{colMsgId} = cte.{colMsgId};";

        var rows = await cnn.QueryAsync<JobMasterRawMessagePersistenceRecord>(deleteSql, new
        {
            Bucket = fullBucketAddressId,
            RefTo = (referenceTimeTo ?? DateTime.UtcNow).ToUniversalTime(),
            Limit = numberOfJobs
        }, tx);

        var ordered = rows
            .OrderBy(r => r.ReferenceTime)
            .ThenBy(r => r.MessageId)
            .ToList();

        return ordered.Select(r => JobMasterRawMessage.RecoverFromDb(r)).ToList();
    }

    // The database clock (SYSUTCDATETIME, like every timestamp column here) sets and checks the lease, so clock skew
    // between workers can't end one early or late. READPAST skips rows another lease is taking right now.
    public override async Task<IList<JobMasterRawMessage>> LeaseMessagesAsync(string fullBucketAddressId, int numberOfJobs, TimeSpan lease)
    {
        var table = MessageTableName();
        var colMsgId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.MessageId);
        var colPayload = sql.ColumnNameFor<JobMasterRawMessage>(x => x.Payload);
        var colRefTime = sql.ColumnNameFor<JobMasterRawMessage>(x => x.ReferenceTime);
        var colCorrId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.CorrelationId);
        var colEnqAt = sql.ColumnNameFor<JobMasterRawMessage>(x => x.EnqueuedAt);
        var colBucket = "bucket_address_id";
        var colLeasedUntil = AgentTableCreatorScripts.LeasedUntilColumn;
        var colLeaseCount = AgentTableCreatorScripts.LeaseCountColumn;

        var leaseSql = $@"
WITH cte AS (
    SELECT TOP (@Limit) {colMsgId}, {colPayload}, {colRefTime}, {colCorrId}, {colEnqAt}, {colLeasedUntil}, {colLeaseCount}
    FROM {table} WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE {colBucket} = @Bucket
      AND ({colLeasedUntil} IS NULL OR {colLeasedUntil} <= SYSUTCDATETIME())
    ORDER BY {colRefTime} ASC, {colMsgId} ASC
)
UPDATE cte
SET {colLeasedUntil} = DATEADD(millisecond, @LeaseMilliseconds, SYSUTCDATETIME()),
    {colLeaseCount} = {colLeaseCount} + 1
OUTPUT INSERTED.{colMsgId}, INSERTED.{colPayload}, INSERTED.{colRefTime}, INSERTED.{colCorrId}, INSERTED.{colEnqAt}, INSERTED.{colLeaseCount};";

        using var cnn = await connManager.OpenAsync(connString, additionalConnConfig);
        var rows = await cnn.QueryAsync<JobMasterRawMessagePersistenceRecord>(leaseSql, new
        {
            Bucket = fullBucketAddressId,
            Limit = numberOfJobs,
            LeaseMilliseconds = (int)lease.TotalMilliseconds,
        });

        return rows
            .OrderBy(r => r.ReferenceTime)
            .ThenBy(r => r.MessageId)
            .Select(r => JobMasterRawMessage.RecoverFromDb(r))
            .ToList();
    }

    public override async Task CreateBucketIfNotExistsAsync(string fullBucketAddressId)
    {
        var table = BucketTableName();
        using var cnn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = cnn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            await cnn.ExecuteAsync($@"
IF NOT EXISTS (SELECT 1 FROM {table} WITH (UPDLOCK, SERIALIZABLE) WHERE bucket_address_id = @Bucket)
    INSERT INTO {table} (bucket_address_id) VALUES (@Bucket);", new { Bucket = fullBucketAddressId }, tx);
            tx.Commit();
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }
}
