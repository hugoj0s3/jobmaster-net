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
    
    protected override async Task<IList<JobMasterRawMessage>> DequeueMessagesAsyncCore(
        IDbConnection cnn, 
        IDbTransaction tx, 
        string fullBucketAddressId, 
        int numberOfJobs, 
        DateTime? referenceTimeTo = null)
    {
        // MySQL-specific: SELECT FOR UPDATE SKIP LOCKED + DELETE (2 operations but with row-level locking)
        var table = MessageTableName();
        var colMsgId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.MessageId);
        var colPayload = sql.ColumnNameFor<JobMasterRawMessage>(x => x.Payload);
        var colRefTime = sql.ColumnNameFor<JobMasterRawMessage>(x => x.ReferenceTime);
        var colCorrId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.CorrelationId);
        var colEnqAt = sql.ColumnNameFor<JobMasterRawMessage>(x => x.EnqueuedAt);
        var colBucket = "bucket_address_id";

        var selectSql = $@"
SELECT {colMsgId}, {colPayload}, {colRefTime}, {colCorrId}, {colEnqAt}
FROM {table}
WHERE {colBucket} = @Bucket";

        if (referenceTimeTo.HasValue)
        {
            selectSql += $" AND {colRefTime} <= @RefTo";
        }

        selectSql += $@"
ORDER BY {colRefTime} ASC, {colMsgId} ASC
LIMIT @Limit
FOR UPDATE SKIP LOCKED;";

        var rows = await cnn.QueryAsync<JobMasterRawMessagePersistenceRecord>(selectSql, new
        {
            Bucket = fullBucketAddressId,
            RefTo = (referenceTimeTo ?? DateTime.UtcNow).ToUniversalTime(),
            Limit = numberOfJobs
        }, tx);

        var picked = rows.ToList();
        var ids = picked.Select(r => r.MessageId).ToList();

        if (ids.Count > 0)
        {
            var deleteSql = $"DELETE FROM {table} WHERE {colBucket} = @Bucket AND {sql.InClauseFor(colMsgId, "@Ids")}";
            await cnn.ExecuteAsync(deleteSql, new { Bucket = fullBucketAddressId, Ids = ids }, tx);
        }

        return picked.Select(r => JobMasterRawMessage.RecoverFromDb(r)).ToList();
    }

    // The database clock (UTC_TIMESTAMP, like every timestamp column here) sets and checks the lease, so clock skew
    // between workers can't end one early or late.
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

        var selectSql = $@"
SELECT {colMsgId}, {colPayload}, {colRefTime}, {colCorrId}, {colEnqAt}, {colLeaseCount}
FROM {table}
WHERE {colBucket} = @Bucket
  AND ({colLeasedUntil} IS NULL OR {colLeasedUntil} <= UTC_TIMESTAMP(6))
ORDER BY {colRefTime} ASC, {colMsgId} ASC
LIMIT @Limit
FOR UPDATE SKIP LOCKED;";

        var updateSql = $@"
UPDATE {table}
SET {colLeasedUntil} = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL @LeaseMicroseconds MICROSECOND),
    {colLeaseCount} = {colLeaseCount} + 1
WHERE {colBucket} = @Bucket AND {sql.InClauseFor(colMsgId, "@Ids")};";

        using var cnn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var tx = cnn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var rows = (await cnn.QueryAsync<JobMasterRawMessagePersistenceRecord>(selectSql, new
            {
                Bucket = fullBucketAddressId,
                Limit = numberOfJobs,
            }, tx)).ToList();

            if (rows.Count > 0)
            {
                await cnn.ExecuteAsync(updateSql, new
                {
                    Bucket = fullBucketAddressId,
                    LeaseMicroseconds = (long)(lease.TotalMilliseconds * 1000),
                    Ids = rows.Select(r => r.MessageId).ToArray(),
                }, tx);
            }

            tx.Commit();

            // Same count the UPDATE just stored: this lease included.
            foreach (var row in rows)
            {
                row.LeaseCount++;
            }

            return rows.Select(r => JobMasterRawMessage.RecoverFromDb(r)).ToList();
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }

    public override async Task CreateBucketIfNotExistsAsync(string fullBucketAddressId)
    {
        using var cnn = await connManager.OpenAsync(connString, additionalConnConfig);
        await cnn.ExecuteAsync(
            $"INSERT INTO {BucketTableName()} (bucket_address_id) VALUES (@Bucket) ON DUPLICATE KEY UPDATE bucket_address_id = bucket_address_id;",
            new { Bucket = fullBucketAddressId });
    }
}
