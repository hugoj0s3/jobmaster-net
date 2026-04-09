using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
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
        int partitionLockId,
        DateTime expiresAtUtc)
    {
        if (partitionLockId <= 0) throw new ArgumentException("partitionLockId must be > 0", nameof(partitionLockId));
        if (queryCriteria == null) throw new ArgumentNullException(nameof(queryCriteria));

        var nowUtcWithSkew = JobMasterConstants.NowUtcWithSkewTolerance();
        var expiresAtUtcKind = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig, ReadIsolationLevel.Consistent);
        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var (whereSql, whereArgs) = BuildWhere(queryCriteria);
            var t = TableName();

            var cId = Col(x => x.Id);
            var cClusterId = Col(x => x.ClusterId);
            var cLastPlanCoverageUntil = Col(x => x.LastPlanCoverageUntil);

            var unlockedGuard =
                $"(s.{Col(x => x.PartitionLockId)} IS NULL OR s.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
            var defaultOrderByClause = $" ORDER BY s.{cLastPlanCoverageUntil} DESC";
            var orderBy = SqlOrderByUtil.BuildOrderByClause(queryCriteria.SortBy, "s", defaultOrderByClause);

            var offsetClause = string.Empty;
            if (queryCriteria.CountLimit > 0)
            {
                offsetClause = "\n" + sql.OffsetQueryFor(queryCriteria.CountLimit, queryCriteria.Offset);
            }

            var selectCols = SelectProjection("s", "e", "v");

            // Step 1: claim rows atomically using FOR UPDATE SKIP LOCKED
            // Avoids UPDATE JOIN lock contention under concurrent workers
            var claimSql = $@"
SELECT s.{cId}
FROM {t} s
{whereSql} AND {unlockedGuard}
{orderBy}
{offsetClause}
FOR UPDATE SKIP LOCKED;";

            var args = new Dictionary<string, object?>();
            foreach (var kv in whereArgs) args[kv.Key] = kv.Value;
            args["ClusterId"] = ClusterConnConfig.ClusterId;
            args["LockNowUtc"] = nowUtcWithSkew;
            args["PartitionLockId"] = partitionLockId;
            args["LockExpiresAt"] = expiresAtUtcKind;

            var claimedIds = (await conn.QueryAsync<Guid>(claimSql, args, tx)).ToList();

            if (claimedIds.Count == 0)
            {
                tx.Commit();
                return new List<RecurringScheduleRawModel>();
            }

            // Step 2: update only the claimed ids
            var inClause = sql.InClauseFor(cId, "@ClaimedIds");
            var updateSql = $@"
UPDATE {t} s
SET s.{Col(x => x.PartitionLockId)} = @PartitionLockId,
    s.{Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    s.{Col(x => x.Version)} = {sql.GenerateVersionSql()}
WHERE s.{cClusterId} = @ClusterId
  AND {inClause};";

            args["ClaimedIds"] = claimedIds;
            await conn.ExecuteAsync(updateSql, args, tx);

            // Step 3: fetch the updated rows with metadata
            var inClauseWithAlias = sql.InClauseFor($"s.{cId}", "@ClaimedIds");
            var fetchSql = $@"
SELECT {selectCols}
FROM {t} s
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = s.{cId}
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.RecurringScheduleMetadata)} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
WHERE s.{cClusterId} = @ClusterId
  AND {inClauseWithAlias}
{orderBy};";

            var linearRows = (await conn.QueryAsync<RecurringSchedulePersistenceRecordLinearDto>(fetchSql, args, tx)).ToList();
            var records = LinearListToDomain(linearRows);
            var result = records.Select(RecurringScheduleRawModel.RecoverFromDb).ToList();

            tx.Commit();
            return result;
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }
    
}