using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Utils;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.MySql.Master;

internal class MySqlMasterRecurringSchedulesRepository : SqlMasterRecurringSchedulesRepository
{
    public MySqlMasterRecurringSchedulesRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;

    public override async Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(
        RecurringScheduleQueryCriteria queryCriteria,
        Guid partitionLockId,
        DateTime expiresAtUtc)
    {
        if (partitionLockId == Guid.Empty) throw new ArgumentException("partitionLockId must be a valid GUID", nameof(partitionLockId));
        if (queryCriteria == null) throw new ArgumentNullException(nameof(queryCriteria));

        var nowUtcWithSkew = JobMasterConstants.NowUtcWithSkewTolerance();
        var expiresAtUtcKind = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);
        var unlockedGuard = $"({Col(x => x.PartitionLockId)} IS NULL OR {Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var (whereSql, args) = BuildWhere(queryCriteria);
            var needsMetadataJoin = queryCriteria.MetadataFilters is { Count: > 0 };

            // Step 1: SELECT candidate IDs with FOR UPDATE SKIP LOCKED.
            // Rows are locked at SELECT time — the subsequent UPDATE uses the explicit
            // ID list, avoiding a nested subquery that MySQL cannot execute under LIMIT.
            var selectIdsSql = BuildQueryIdsToLockSql(
                whereSql, needsMetadataJoin,
                queryCriteria.CountLimit, queryCriteria.Offset, queryCriteria.SortBy);

            var selectIdsArgs = new Dictionary<string, object?>(args);
            selectIdsArgs["LockNowUtc"] = nowUtcWithSkew;
            if (needsMetadataJoin)
                selectIdsArgs["GroupId"] = MasterGenericRecordGroupIds.RecurringScheduleMetadata;

            var ids = (await conn.QueryAsync<Guid>(selectIdsSql, selectIdsArgs, trans)).ToList();

            if (ids.Count == 0)
            {
                trans.Commit();
                return new List<RecurringScheduleRawModel>();
            }

            // Step 2: Point-UPDATE using the explicit ID list.
            // FOR UPDATE locks from step 1 guarantee no competing transaction can
            // modify these rows between the SELECT and this UPDATE.
            var t = TableName();
            var inClause = sql.InClauseFor(Col(x => x.Id), "@Ids");
            var updateSql = $@"
UPDATE {t}
SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
    {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    {Col(x => x.Version)} = {sql.GenerateVersionSql()}
WHERE {Col(x => x.ClusterId)} = @ClusterId
  AND {inClause}
  AND {unlockedGuard};";

            var updateArgs = new Dictionary<string, object?>
            {
                { "ClusterId", ClusterConnConfig.ClusterId },
                { "Ids", ids },
                { "PartitionLockId", partitionLockId },
                { "LockExpiresAt", expiresAtUtcKind },
                { "LockNowUtc", nowUtcWithSkew }
            };

            await conn.ExecuteAsync(updateSql, updateArgs, trans);

            // Step 3: Fetch full records on the same connection and transaction,
            // filtered by partitionLockId to confirm each row was actually locked.
            var schedules = await FetchSchedulesByPartitionLockAsync(ids, partitionLockId, nowUtcWithSkew, conn, trans);

            // Partial guard: fewer rows returned than locked means something unexpected
            // happened. Roll back rather than returning a short batch.
            if (schedules.Count != ids.Count)
            {
                trans.SafeRollback();
                return new List<RecurringScheduleRawModel>();
            }

            trans.Commit();
            return schedules;
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    private async Task<IList<RecurringScheduleRawModel>> FetchSchedulesByPartitionLockAsync(
        List<Guid> ids,
        Guid partitionLockId,
        DateTime nowUtcWithSkew,
        IDbConnection conn,
        IDbTransaction trans)
    {
        var selectCols = SelectProjection();
        var t = TableName();
        var inClause = sql.InClauseFor($"s.{Col(x => x.Id)}", "@Ids");

        var sqlText = $@"
SELECT {selectCols}
FROM {t} s
WHERE s.{Col(x => x.ClusterId)} = @ClusterId
  AND {inClause}
  AND s.{Col(x => x.PartitionLockId)} = @PartitionLockId
  AND s.{Col(x => x.PartitionLockExpiresAt)} > @NowUtcWithSkew";

        var fetchArgs = new Dictionary<string, object?>
        {
            { "ClusterId", ClusterConnConfig.ClusterId },
            { "Ids", ids },
            { "PartitionLockId", partitionLockId },
            { "NowUtcWithSkew", nowUtcWithSkew }
        };

        var linearRows = (await conn.QueryAsync<RecurringSchedulePersistenceRecordLinearDto>(sqlText, fetchArgs, trans)).ToList();
        var rows = LinearListToDomain(linearRows);
        return rows.Select(RecurringScheduleRawModel.RecoverFromDb).ToList();
    }

    protected override string BuildQueryIdsToLockSql(
        string whereSql,
        bool needsMetadataJoin,
        int countLimit,
        int offset,
        SortByCriteria? sortByCriteria)
    {
        // Push the unlocked guard into the inner WHERE so LIMIT applies only to
        // rows that are actually acquirable. Without this, already-locked rows
        // consume LIMIT slots and the batch comes back smaller than expected.
        // @LockNowUtc is provided by AcquireAndFetchAsync's selectIdsArgs.
        var unlockedFilter = $" AND ({Col(x => x.PartitionLockId)} IS NULL " +
                             $"OR {Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
        var whereWithUnlocked = whereSql + unlockedFilter;

        var baseSql = base.BuildQueryIdsToLockSql(whereWithUnlocked, needsMetadataJoin, countLimit, offset, sortByCriteria);

        // MySQL does not allow LIMIT inside an IN subquery directly.
        // Wrapping in a derived table is the standard workaround.
        // FOR UPDATE SKIP LOCKED inside the derived table gives race-free candidate
        // selection — competing workers skip already-locked rows at SELECT time.
        return $"SELECT {Col(x => x.Id)} FROM ({baseSql} FOR UPDATE SKIP LOCKED) AS _candidates";
    }

}