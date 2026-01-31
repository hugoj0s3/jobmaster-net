using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;
using Npgsql;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterJobsRepository : SqlMasterJobsRepository
{
    public PostgresMasterJobsRepository(JobMasterClusterConnectionConfig clusterConnectionConfig, IDbConnectionManager connectionManager) : 
        base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;

    public override async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, int partitionLockId, DateTime expiresAtUtc)
    {
        if (partitionLockId <= 0) throw new ArgumentException("partitionLockId must be > 0", nameof(partitionLockId));
        if (queryCriteria == null) throw new ArgumentNullException(nameof(queryCriteria));

        var nowUtcWithSkew = JobMasterConstants.NowUtcWithSkewTolerance();

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig, ReadIsolationLevel.Consistent);
        using var tx = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
        try
        {
            var (whereSql, whereArgs) = BuildWhere(queryCriteria);
            var t = TableName();

            var cId = Col(x => x.Id);
            var cClusterId = Col(x => x.ClusterId);
            var cScheduledAt = Col(x => x.ScheduledAt);
            var cCreatedAt = Col(x => x.CreatedAt);

            var unlockedGuard = $"(j.{Col(x => x.PartitionLockId)} IS NULL OR j.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
            var orderBy = $"j.{cScheduledAt} ASC, j.{cCreatedAt} ASC";

            var offsetClause = string.Empty;
            if (queryCriteria.CountLimit > 0)
            {
                offsetClause = "\n" + sql.OffsetQueryFor(queryCriteria.CountLimit, queryCriteria.Offset);
            }

            var selectCols = SelectProjection();

            var sqlText = $@"
WITH candidates AS (
    SELECT j.{cId} AS id
    FROM {t} j
    {whereSql} AND {unlockedGuard}
    ORDER BY {orderBy}
    {offsetClause}
), locked AS (
    UPDATE {t} j
    SET {Col(x => x.PartitionLockId)} = @PartitionLockId,
        {Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
        {Col(x => x.Version)} = {sql.GenerateVersionSql()}
    FROM candidates c
    WHERE j.{cClusterId} = @ClusterId
      AND j.{cId} = c.id
      AND {unlockedGuard}
    RETURNING j.*
)
SELECT {selectCols}
FROM locked j
LEFT JOIN {genericUtil.EntryTable()} e ON e.{Col(x => x.EntryIdGuid)} = j.{cId} and e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable()} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
ORDER BY {orderBy};";

            var args = new Dictionary<string, object?>();
            foreach (var kv in whereArgs) args[kv.Key] = kv.Value;
            args["ClusterId"] = ClusterConnConfig.ClusterId;
            args["PartitionLockId"] = partitionLockId;
            args["LockExpiresAt"] = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);
            args["LockNowUtc"] = nowUtcWithSkew;
            args["GroupId"] = MasterGenericRecordGroupIds.JobMetadata;

            var linearRows = (await conn.QueryAsync<JobPersistenceRecordLinearDto>(sqlText, args, tx)).ToList();
            var records = LinearListRecord(linearRows);
            var result = records.Select(JobRawModel.RecoverFromDb).ToList();

            tx.Commit();
            return result;
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    protected override bool IsDupeViolation(Guid jobId, Exception ex)
    {
        return ex is PostgresException pgEx && pgEx.SqlState == "23505";
    }
}
