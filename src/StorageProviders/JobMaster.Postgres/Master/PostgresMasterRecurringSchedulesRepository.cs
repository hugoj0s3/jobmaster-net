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

namespace JobMaster.Postgres.Master;

internal class PostgresMasterRecurringSchedulesRepository : SqlMasterRecurringSchedulesRepository
{
    public PostgresMasterRecurringSchedulesRepository(JobMasterClusterConnectionConfig clusterConnectionConfig, IDbConnectionManager connectionManager) :
        base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;

    protected override string BuildQueryIdsToLockSql(string whereSql, bool needsMetadataJoin, int countLimit, int offset,
        SortByCriteria? sortByCriteria)
    {
        var baseSql = base.BuildQueryIdsToLockSql(whereSql, needsMetadataJoin, countLimit, offset, sortByCriteria);
        return baseSql + " FOR UPDATE SKIP LOCKED";
    }

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
            var t = TableName();
            var (whereSql, args) = BuildWhere(queryCriteria);
            args["LockNowUtc"] = nowUtcWithSkew;
            var acquireWhereSql = whereSql + $" AND (s.{Col(x => x.PartitionLockId)} IS NULL OR s.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
            var needsMetadataJoin = queryCriteria.MetadataFilters is { Count: > 0 };
            var queryIdsSql = BuildQueryIdsToLockSql(acquireWhereSql, needsMetadataJoin, queryCriteria.CountLimit, queryCriteria.Offset, queryCriteria.SortBy);

            // Postgres-specific: nesting `id IN (subquery ... FOR UPDATE SKIP LOCKED)`
            // directly inside this table's own UPDATE disables Postgres's usual subquery
            // materialization (FOR UPDATE is volatile), so the planner falls back to a
            // Nested Loop Semi Join that re-executes the LIMIT-ed candidate subquery once
            // per outer row scanned, silently claiming up to CountLimit rows PER outer row
            // instead of once overall. Wrapping the candidate select in a CTE forces single
            // evaluation -- the same idiom already used in PostgresRawMessagesDispatcherRepository
            // and PostgresMasterJobsRepository.
            var updateSql = $@"
WITH candidate AS (
{queryIdsSql}
)
UPDATE {t}
SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
    {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    {Col(x => x.Version)} = {sql.GenerateVersionSql()}
FROM candidate
WHERE {t}.{Col(x => x.ClusterId)} = @ClusterId
  AND {t}.{Col(x => x.Id)} = candidate.{Col(x => x.Id)}
  AND {unlockedGuard};";

            var args2 = new Dictionary<string, object?>(args);
            args2["LockNowUtc"] = nowUtcWithSkew;
            args2["LockExpiresAt"] = expiresAtUtcKind;
            args2["PartitionLockId"] = partitionLockId;
            if (needsMetadataJoin)
                args2["GroupId"] = MasterGenericRecordGroupIds.RecurringScheduleMetadata;

            var rowsAffected = await conn.ExecuteAsync(updateSql, args2, trans);

            trans.Commit();

            if (rowsAffected == 0) return new List<RecurringScheduleRawModel>();

            using var conn2 = await connManager.OpenAsync(connString, additionalConnConfig);
            return await QueryLockedSchedulesAsync(partitionLockId, nowUtcWithSkew, conn2);
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }
}
