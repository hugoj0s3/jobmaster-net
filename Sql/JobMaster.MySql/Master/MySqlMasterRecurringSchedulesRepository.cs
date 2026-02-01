using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;

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
            var cCreatedAt = Col(x => x.CreatedAt);

            var unlockedGuard = $"(s.{Col(x => x.PartitionLockId)} IS NULL OR s.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
            var orderBy = $"s.{cLastPlanCoverageUntil} DESC, s.{cCreatedAt} ASC";

            var offsetClause = string.Empty;
            if (queryCriteria.CountLimit > 0)
            {
                offsetClause = "\n" + sql.OffsetQueryFor(queryCriteria.CountLimit, queryCriteria.Offset);
            }

            var selectCols = SelectProjection("s", "e", "v");

            var sqlText = $@"
UPDATE {t} s
JOIN (
    SELECT s.{cId} AS id
    FROM {t} s
    {whereSql} AND {unlockedGuard}
    ORDER BY {orderBy}
    {offsetClause}
) c ON c.id = s.{cId}
SET s.{Col(x => x.PartitionLockId)} = @PartitionLockId,
    s.{Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    s.{Col(x => x.Version)} = {sql.GenerateVersionSql()}
WHERE s.{cClusterId} = @ClusterId
  AND {unlockedGuard};

SELECT {selectCols}
FROM {t} s
LEFT JOIN {genericUtil.EntryTable()} e ON e.{Col(x => x.EntryIdGuid)} = s.{cId}
LEFT JOIN {genericUtil.EntryValueTable()} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
WHERE s.{cClusterId} = @ClusterId
  AND s.{Col(x => x.PartitionLockId)} = @PartitionLockId
  AND s.{Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt
ORDER BY {orderBy}
{offsetClause};";

            var args = new Dictionary<string, object?>();
            foreach (var kv in whereArgs) args[kv.Key] = kv.Value;
            args["ClusterId"] = ClusterConnConfig.ClusterId;
            args["PartitionLockId"] = partitionLockId;
            args["LockExpiresAt"] = expiresAtUtcKind;
            args["LockNowUtc"] = nowUtcWithSkew;

            var linearRows = (await conn.QueryAsync<RecurringSchedulePersistenceRecordLinearDto>(sqlText, args, tx)).ToList();
            var records = LinearListToDomain(linearRows);
            return records.Select(RecurringScheduleRawModel.RecoverFromDb).ToList();
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }
}
