using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;
using MySqlConnector;

namespace JobMaster.MySql.Master;

internal class MySqlMasterJobsRepository : SqlMasterJobsRepository
{
    public MySqlMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;

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
            var cNextPlanExecutionAt = Col(x => x.NextPlanExecutionAt);

            var unlockedGuard =
                $"(j.{Col(x => x.PartitionLockId)} IS NULL OR j.{Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
            var defaultOrderByClause = $" ORDER BY j.{cNextPlanExecutionAt} ASC";
            var orderBy = SqlOrderByUtil.BuildOrderByClause(queryCriteria.SortBy, "j", defaultOrderByClause);

            var offsetClause = string.Empty;
            if (queryCriteria.CountLimit > 0)
            {
                offsetClause = "\n" + sql.OffsetQueryFor(queryCriteria.CountLimit, queryCriteria.Offset);
            }

            var selectCols = SelectProjection();

            // Step 1: claim rows atomically using FOR UPDATE SKIP LOCKED
            // This avoids the UPDATE JOIN pattern which causes lock contention and timeouts
            // under concurrent workers. SKIP LOCKED means competing workers skip already-locked rows
            // instead of waiting, eliminating the timeout.
            var claimSql = $@"
SELECT j.{cId}
FROM {t} j
{whereSql} AND {unlockedGuard}
{orderBy}
{offsetClause}
FOR UPDATE SKIP LOCKED;";

            var args = new Dictionary<string, object?>();
            foreach (var kv in whereArgs) args[kv.Key] = kv.Value;
            args["ClusterId"] = ClusterConnConfig.ClusterId;
            args["LockNowUtc"] = nowUtcWithSkew;
            args["PartitionLockId"] = partitionLockId;
            args["LockExpiresAt"] = DateTime.SpecifyKind(expiresAtUtc, DateTimeKind.Utc);
            args["GroupId"] = MasterGenericRecordGroupIds.JobMetadata;

            var claimedIds = (await conn.QueryAsync<Guid>(claimSql, args, tx)).ToList();

            if (claimedIds.Count == 0)
            {
                tx.Commit();
                return new List<JobRawModel>();
            }

            // Step 2: update only the claimed ids
            var inClause = sql.InClauseFor(cId, "@ClaimedIds");
            var updateSql = $@"
UPDATE {t} j
SET j.{Col(x => x.PartitionLockId)} = @PartitionLockId,
    j.{Col(x => x.PartitionLockExpiresAt)} = @LockExpiresAt,
    j.{Col(x => x.Version)} = {sql.GenerateVersionSql()}
WHERE j.{cClusterId} = @ClusterId
  AND {inClause};";

            args["ClaimedIds"] = claimedIds;
            await conn.ExecuteAsync(updateSql, args, tx);

            // Step 3: fetch the updated rows with metadata
            var inClauseWithAlias = sql.InClauseFor($"j.{cId}", "@ClaimedIds");
            var fetchSql = $@"
SELECT {selectCols}
FROM {t} j
LEFT JOIN {genericUtil.EntryTable(MasterGenericRecordGroupIds.JobMetadata)} e ON e.{Col(x => x.EntryIdGuid)} = j.{cId} AND e.{Col(x => x.GroupId)} = @GroupId
LEFT JOIN {genericUtil.EntryValueTable(MasterGenericRecordGroupIds.JobMetadata)} v ON v.{Col(x => x.RecordUniqueId)} = e.{Col(x => x.RecordUniqueId)}
WHERE j.{cClusterId} = @ClusterId
  AND {inClauseWithAlias}
{orderBy};";

            var linearRows = (await conn.QueryAsync<JobPersistenceRecordLinearDto>(fetchSql, args, tx)).ToList();
            var records = LinearListRecord(linearRows);
            var result = records.Select(JobRawModel.RecoverFromDb).ToList();

            tx.Commit();
            return result;
        }
        catch
        {
            tx.SafeRollback();
            throw;
        }
    }

    protected override bool IsDupeViolation(Guid jobId, Exception ex)
    {
        if (ex is MySqlException mysqlEx)
        {
            // Strict: only treat canonical ER_DUP_ENTRY as duplication
            return mysqlEx.Number == 1062
                   || mysqlEx.ErrorCode == MySqlErrorCode.DuplicateKeyEntry;
        }

        return false; // no inner recursion to avoid false positives
    }
    
}