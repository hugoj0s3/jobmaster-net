using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;

namespace JobMaster.SqlServer.Master;

internal class SqlServerMasterRecurringSchedulesRepository : SqlMasterRecurringSchedulesRepository
{
    public SqlServerMasterRecurringSchedulesRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;

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
DECLARE @lockedIds TABLE (id uniqueidentifier NOT NULL);

;WITH candidates AS (
    SELECT s.{cId} AS id
    FROM {t} s
    {whereSql} AND {unlockedGuard}
    ORDER BY {orderBy}
    {offsetClause}
)
UPDATE s
SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
    {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    {Col(x => x.Version)} = {sql.GenerateVersionSql()}
OUTPUT inserted.{cId} INTO @lockedIds(id)
FROM {t} s
JOIN candidates c ON c.id = s.{cId}
WHERE s.{cClusterId} = @ClusterId
  AND {unlockedGuard};

SELECT {selectCols}
FROM {t} s
JOIN @lockedIds l ON l.id = s.{cId}
LEFT JOIN {genericUtil.EntryTable()} e ON e.{Col(x => x.EntryIdGuid)} = s.{cId}
LEFT JOIN {genericUtil.EntryValueTable()} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
ORDER BY {orderBy};";

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
