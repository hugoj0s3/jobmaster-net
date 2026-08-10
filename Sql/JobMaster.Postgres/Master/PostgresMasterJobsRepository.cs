using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.SqlBase.Connections;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;
using JobMaster.Sdk.Utils;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterJobsRepository : SqlMasterJobsRepository
{
    public PostgresMasterJobsRepository(JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager,
        IKnownExceptionIdentifier knownExceptionIdentifier) :
        base(clusterConnectionConfig, connectionManager, knownExceptionIdentifier)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;

    protected override string BuildQueryIdsToLockSql(string whereSql, bool needsMetadataJoin, int countLimit, int offset,
        SortByCriteria? sortByCriteria)
    {
        var baseSql = base.BuildQueryIdsToLockSql(whereSql, needsMetadataJoin, countLimit, offset, sortByCriteria);
        return baseSql + " FOR UPDATE SKIP LOCKED";
    }

    public override async Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria queryCriteria, Guid partitionLockId,
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
            var (whereSql, args) = BuildWhere(queryCriteria, isLocked: false);
            var needsMetadataJoin = queryCriteria.MetadataFilters is { Count: > 0 };
            var queryIdsSql =
                BuildQueryIdsToLockSql(whereSql, needsMetadataJoin, queryCriteria.CountLimit, queryCriteria.Offset, queryCriteria.SortBy);

            // Postgres-specific: nesting `id IN (subquery ... FOR UPDATE SKIP LOCKED)`
            // directly inside this table's own UPDATE disables Postgres's usual subquery
            // materialization (FOR UPDATE is volatile), so the planner falls back to a
            // Nested Loop Semi Join that re-executes the LIMIT-ed candidate subquery once
            // per outer row scanned, silently claiming up to CountLimit rows PER outer row
            // instead of once overall (confirmed via EXPLAIN ANALYZE: "loops=N" on the
            // Limit node). Wrapping the candidate select in a CTE forces single evaluation
            // -- the same idiom already used in PostgresRawMessagesDispatcherRepository.
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
  AND {unlockedGuard}
  ;";
            var args2 = new Dictionary<string, object?>(args);
            args2.Add("LockNowUtc", nowUtcWithSkew);
            args2.Add("LockExpiresAt", expiresAtUtcKind);
            args2.Add("PartitionLockId", partitionLockId);

            var rowsAffected = await conn.ExecuteAsync(updateSql, args2, trans);

            trans.Commit();

            if (rowsAffected == 0)
            {
                return new List<JobRawModel>();
            }

            using var conn2 = await connManager.OpenAsync(connString, additionalConnConfig);
            var result = await QueryLockedJobsAsync(partitionLockId, nowUtcWithSkew, conn2);
            return result;
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }


    // Postgres-specific: one multi-row UPDATE ... FROM (VALUES ...) instead of N sequential
    // single-row UPDATEs. RETURNING gives back exactly which ids were touched in the same
    // round trip, so no follow-up query is needed here (unlike MySQL).
    public override async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs)
    {
        if (jobs.Count == 0) return Array.Empty<JobRawModel>();

        var newVersions = jobs.ToDictionary(j => j.Id, _ => JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant());

        var p = new DynamicParameters();
        p.Add("ClusterId", ClusterConnConfig.ClusterId, DbType.String);
        var valuesRows = new List<string>(jobs.Count);
        for (var i = 0; i < jobs.Count; i++)
        {
            var rec = JobRawModel.ToPersistence(jobs[i]);
            var expectedVersion = rec.Version;
            rec.Version = newVersions[jobs[i].Id];
            AddBulkUpdateRowParams(p, i, rec, expectedVersion);
            valuesRows.Add($"(@Id_{i}, @JobDefinitionId_{i}, @TriggerSourceType_{i}, @BucketId_{i}, " +
                            $"@AgentConnectionId_{i}, @AgentWorkerId_{i}, @Priority_{i}, @NextPlanExecutionAt_{i}, " +
                            $"@ScheduledAt_{i}, @MsgData_{i}, @Status_{i}, @NumberOfFailures_{i}, @TimeoutTicks_{i}, " +
                            $"@MaxNumberOfRetries_{i}, @SourceId_{i}, @PartitionLockId_{i}, @PartitionLockExpiresAt_{i}, " +
                            $"@ProcessDeadline_{i}, @ProcessStartedAt_{i}, @FinalizedAt_{i}, @WorkerLane_{i}, " +
                            $"@Version_{i}, @HostId_{i}, @HostDisplayName_{i}, @ExpectedVersion_{i})");
        }

        var t = TableName();
        var cId = Col(x => x.Id);
        var sqlText = $@"
UPDATE {t}
SET {Col(x => x.JobDefinitionId)} = v.jobdefinitionid,
    {Col(x => x.TriggerSourceType)} = v.triggersourcetype,
    {Col(x => x.BucketId)} = v.bucketid,
    {Col(x => x.AgentConnectionId)} = v.agentconnectionid,
    {Col(x => x.AgentWorkerId)} = v.agentworkerid,
    {Col(x => x.Priority)} = v.priority,
    {Col(x => x.NextPlanExecutionAt)} = v.nextplanexecutionat,
    {Col(x => x.ScheduledAt)} = v.scheduledat,
    {Col(x => x.MsgData)} = v.msgdata,
    {Col(x => x.Status)} = v.status,
    {Col(x => x.NumberOfFailures)} = v.numberoffailures,
    {Col(x => x.TimeoutTicks)} = v.timeoutticks,
    {Col(x => x.MaxNumberOfRetries)} = v.maxnumberofretries,
    {Col(x => x.SourceId)} = v.sourceid,
    {Col(x => x.PartitionLockId)} = v.partitionlockid,
    {Col(x => x.PartitionLockExpiresAt)} = v.partitionlockexpiresat,
    {Col(x => x.ProcessDeadline)} = v.processdeadline,
    {Col(x => x.ProcessStartedAt)} = v.processstartedat,
    {Col(x => x.FinalizedAt)} = v.finalizedat,
    {Col(x => x.WorkerLane)} = v.workerlane,
    {Col(x => x.Version)} = v.version,
    {Col(x => x.HostId)} = v.hostid,
    {Col(x => x.HostDisplayName)} = v.hostdisplayname
FROM (VALUES {string.Join(",\n", valuesRows)}) AS v(id, jobdefinitionid, triggersourcetype, bucketid,
    agentconnectionid, agentworkerid, priority, nextplanexecutionat, scheduledat, msgdata, status,
    numberoffailures, timeoutticks, maxnumberofretries, sourceid, partitionlockid, partitionlockexpiresat,
    processdeadline, processstartedat, finalizedat, workerlane, version, hostid, hostdisplayname, expectedversion)
WHERE {t}.{Col(x => x.ClusterId)} = @ClusterId AND {t}.{cId} = v.id
  AND {t}.{Col(x => x.Version)} = v.expectedversion
RETURNING {t}.{cId};";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var updatedIds = new HashSet<Guid>(await conn.QueryAsync<Guid>(sqlText, p, trans));
            trans.Commit();

            var updated = jobs.Where(j => updatedIds.Contains(j.Id)).ToList();
            foreach (var job in updated)
            {
                job.SetVersion(newVersions[job.Id]);
            }
            return updated;
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    private string BuildJobUpsertSql()
    {
        var t = TableName();
        var (cols, vals) = InsertColumnsAndParams();
        var conflictCols = $"{Col(x => x.ClusterId)}, {Col(x => x.Id)}";
        var cVersion = Col(x => x.Version);
        return $@"
INSERT INTO {t} ({cols}) VALUES ({vals})
ON CONFLICT ({conflictCols}) DO UPDATE SET
    {UpdateSetClause()}
WHERE {t}.{cVersion} = @ExpectedVersion;";
    }
}