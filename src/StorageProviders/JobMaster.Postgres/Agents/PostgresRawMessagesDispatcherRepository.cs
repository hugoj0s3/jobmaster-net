using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.SqlBase.Agents;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Scripts;
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
        // Postgres-specific: ensure deterministic order by selecting IDs ordered, deleting, then returning ordered rows
        var table = MessageTableName();
        var colMsgId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.MessageId);
        var colPayload = sql.ColumnNameFor<JobMasterRawMessage>(x => x.Payload);
        var colRefTime = sql.ColumnNameFor<JobMasterRawMessage>(x => x.ReferenceTime);
        var colCorrId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.CorrelationId);
        var colEnqAt = sql.ColumnNameFor<JobMasterRawMessage>(x => x.EnqueuedAt);
        var colBucket = "bucket_address_id";

        var deleteSql = $@"
WITH cte AS (
    SELECT {colMsgId}
    FROM {table}
    WHERE {colBucket} = @Bucket
";

        if (referenceTimeTo.HasValue)
        {
            deleteSql += $"    AND {colRefTime} <= @RefTo\n";
        }

        deleteSql += $@"    ORDER BY {colRefTime} ASC, {colMsgId} ASC
    LIMIT @Limit
    FOR UPDATE SKIP LOCKED
), deleted AS (
    DELETE FROM {table} t
    USING cte
    WHERE t.{colMsgId} = cte.{colMsgId}
    RETURNING t.{colMsgId} AS {colMsgId}, t.{colPayload} AS {colPayload}, t.{colRefTime} AS {colRefTime}, t.{colCorrId} AS {colCorrId}, t.{colEnqAt} AS {colEnqAt}
)
SELECT {colMsgId}, {colPayload}, {colRefTime}, {colCorrId}, {colEnqAt}
FROM deleted
ORDER BY {colRefTime} ASC, {colMsgId} ASC;";

        var rows = await cnn.QueryAsync<JobMasterRawMessagePersistenceRecord>(deleteSql, new
        {
            Bucket = fullBucketAddressId,
            RefTo = (referenceTimeTo ?? DateTime.UtcNow).ToUniversalTime(),
            Limit = numberOfJobs
        }, tx);

        return rows.Select(r => JobMasterRawMessage.RecoverFromDb(r)).ToList();
    }

    // The database clock (UTC, like every timestamp column here) sets and checks the lease, so clock skew between
    // workers can't end one early or late.
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
    SELECT {colMsgId}
    FROM {table}
    WHERE {colBucket} = @Bucket
      AND ({colLeasedUntil} IS NULL OR {colLeasedUntil} <= (now() AT TIME ZONE 'utc'))
    ORDER BY {colRefTime} ASC, {colMsgId} ASC
    LIMIT @Limit
    FOR UPDATE SKIP LOCKED
), leased AS (
    UPDATE {table} t
    SET {colLeasedUntil} = (now() AT TIME ZONE 'utc') + make_interval(secs => @LeaseSeconds),
        {colLeaseCount} = t.{colLeaseCount} + 1
    FROM cte
    WHERE t.{colMsgId} = cte.{colMsgId}
    RETURNING t.{colMsgId} AS {colMsgId}, t.{colPayload} AS {colPayload}, t.{colRefTime} AS {colRefTime}, t.{colCorrId} AS {colCorrId}, t.{colEnqAt} AS {colEnqAt}, t.{colLeaseCount} AS {colLeaseCount}
)
SELECT {colMsgId}, {colPayload}, {colRefTime}, {colCorrId}, {colEnqAt}, {colLeaseCount}
FROM leased
ORDER BY {colRefTime} ASC, {colMsgId} ASC;";

        using var cnn = await connManager.OpenAsync(connString, additionalConnConfig);
        var rows = await cnn.QueryAsync<JobMasterRawMessagePersistenceRecord>(leaseSql, new
        {
            Bucket = fullBucketAddressId,
            Limit = numberOfJobs,
            LeaseSeconds = lease.TotalSeconds,
        });

        return rows.Select(r => JobMasterRawMessage.RecoverFromDb(r)).ToList();
    }

    public override async Task CreateBucketIfNotExistsAsync(string fullBucketAddressId)
    {
        using var cnn = await connManager.OpenAsync(connString, additionalConnConfig);
        await cnn.ExecuteAsync(
            $"INSERT INTO {BucketTableName()} (bucket_address_id) VALUES (@Bucket) ON CONFLICT DO NOTHING;",
            new { Bucket = fullBucketAddressId });
    }
}