using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Scripts;
using System.Data;
using JobMaster.Sdk.Utils;

namespace JobMaster.MySql.Master;

internal class MySqlMasterJobsRepository : SqlMasterJobsRepository
{
    public MySqlMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager,
        IKnownExceptionIdentifier knownExceptionIdentifier) : base(clusterConnectionConfig, connectionManager, knownExceptionIdentifier)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;
    
    public override async Task<IList<JobRawModel>> AcquireAndFetchAsync(
        JobQueryCriteria queryCriteria,
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

            // Step 1: Execute the id SELECT directly on the connection.
            // FOR UPDATE SKIP LOCKED acquires exclusive row locks and returns only
            // rows that are actually acquirable — no subquery inside the UPDATE.
            var selectIdsSql = BuildQueryIdsToLockSql(
                whereSql, needsMetadataJoin,
                queryCriteria.CountLimit, queryCriteria.Offset, queryCriteria.SortBy);

            var selectIdsArgs = new Dictionary<string, object?>(args);
            selectIdsArgs["LockNowUtc"] = nowUtcWithSkew;
            if (needsMetadataJoin)
                selectIdsArgs["GroupId"] = MasterGenericRecordGroupIds.JobMetadata;

            var ids = (await conn.QueryAsync<Guid>(selectIdsSql, selectIdsArgs, trans)).ToList();

            if (ids.Count == 0)
            {
                trans.Commit();
                return new List<JobRawModel>();
            }

            // Step 2: Point-UPDATE using the explicit ID list.
            // The FOR UPDATE locks from step 1 guarantee no other transaction can
            // touch these rows — no nested subquery needed in the UPDATE.
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

            // Step 3: Fetch full records on the same connection and transaction.
            // Filters by partition_lock_id so only rows the UPDATE actually locked
            // are returned — driver-agnostic partial lock detection.
            var jobs = await FetchJobsByPartitionLockAsync(ids, partitionLockId, nowUtcWithSkew, conn, trans);

            // Partial guard: if fewer rows came back than we locked, something
            // unexpected happened. Roll back cleanly instead of returning a short
            // batch that triggers the fallback cascade downstream.
            if (jobs.Count != ids.Count)
            {
                trans.SafeRollback();
                return new List<JobRawModel>();
            }

            trans.Commit();
            return jobs;
        }
        catch
        {
            trans.SafeRollback();
            throw;
        }
    }

    private async Task<IList<JobRawModel>> FetchJobsByPartitionLockAsync(
        List<Guid> ids,
        Guid partitionLockId,
        DateTime nowUtcWithSkew,
        System.Data.IDbConnection conn,
        System.Data.IDbTransaction trans)
    {
        var selectCols = SelectProjection();
        var t = TableName();
        var inClause = sql.InClauseFor($"j.{Col(x => x.Id)}", "@Ids");

        var sqlText = $@"
SELECT {selectCols}
FROM {t} j
WHERE j.{Col(x => x.ClusterId)} = @ClusterId
  AND {inClause}
  AND j.{Col(x => x.PartitionLockId)} = @PartitionLockId
  AND j.{Col(x => x.PartitionLockExpiresAt)} > @NowUtcWithSkew";

        var fetchArgs = new Dictionary<string, object?>
        {
            { "ClusterId", ClusterConnConfig.ClusterId },
            { "Ids", ids },
            { "PartitionLockId", partitionLockId },
            { "NowUtcWithSkew", nowUtcWithSkew }
        };

        var linearRows = (await conn.QueryAsync<JobPersistenceRecordLinearDto>(sqlText, fetchArgs, trans)).ToList();
        var rows = LinearListRecord(linearRows);
        return rows.Select(JobRawModel.RecoverFromDb).ToList();
    }

    // MySQL-specific: one multi-row UPDATE ... JOIN (derived table built from UNION ALL SELECTs)
    // instead of N sequential single-row UPDATEs. Unlike Postgres/SQL Server, MySQL has no multi-row
    // RETURNING/OUTPUT, so a follow-up query tells us which ids actually got updated. That follow-up
    // re-joins the same derived table against t.version (not just existence) -- since new versions
    // are freshly generated GUIDs never seen before, a match proves the UPDATE's own version-match
    // join condition passed for that row, i.e. it wasn't skipped for carrying a stale/mismatched version.
    public override async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs)
    {
        if (jobs.Count == 0) return Array.Empty<JobRawModel>();

        var newVersions = jobs.ToDictionary(j => j.Id, _ => JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant());

        var p = new DynamicParameters();
        p.Add("ClusterId", ClusterConnConfig.ClusterId, DbType.String);
        var selects = new List<string>(jobs.Count);
        for (var i = 0; i < jobs.Count; i++)
        {
            var rec = JobRawModel.ToPersistence(jobs[i]);
            var expectedVersion = rec.Version;
            rec.Version = newVersions[jobs[i].Id];
            AddBulkUpdateRowParams(p, i, rec, expectedVersion);
            selects.Add($"SELECT @Id_{i} AS id, @JobDefinitionId_{i} AS jobdefinitionid, @TriggerSourceType_{i} AS triggersourcetype, " +
                        $"@BucketId_{i} AS bucketid, @AgentConnectionId_{i} AS agentconnectionid, @AgentWorkerId_{i} AS agentworkerid, " +
                        $"@Priority_{i} AS priority, @NextPlanExecutionAt_{i} AS nextplanexecutionat, @ScheduledAt_{i} AS scheduledat, " +
                        $"@MsgData_{i} AS msgdata, @Status_{i} AS status, @NumberOfFailures_{i} AS numberoffailures, " +
                        $"@TimeoutTicks_{i} AS timeoutticks, @MaxNumberOfRetries_{i} AS maxnumberofretries, @SourceId_{i} AS sourceid, " +
                        $"@PartitionLockId_{i} AS partitionlockid, @PartitionLockExpiresAt_{i} AS partitionlockexpiresat, " +
                        $"@ProcessDeadline_{i} AS processdeadline, @ProcessStartedAt_{i} AS processstartedat, @FinalizedAt_{i} AS finalizedat, " +
                        $"@WorkerLane_{i} AS workerlane, @Version_{i} AS version, @HostId_{i} AS hostid, @HostDisplayName_{i} AS hostdisplayname, " +
                        $"@ExpectedVersion_{i} AS expectedversion");
        }

        var t = TableName();
        var cId = Col(x => x.Id);
        var cVersion = Col(x => x.Version);
        var derivedTable = $"{string.Join("\nUNION ALL\n", selects)}";
        var updateSql = $@"
UPDATE {t} AS t
JOIN (
{derivedTable}
) AS v ON t.{cId} = v.id AND t.{cVersion} = v.expectedversion
SET t.{Col(x => x.JobDefinitionId)} = v.jobdefinitionid,
    t.{Col(x => x.TriggerSourceType)} = v.triggersourcetype,
    t.{Col(x => x.BucketId)} = v.bucketid,
    t.{Col(x => x.AgentConnectionId)} = v.agentconnectionid,
    t.{Col(x => x.AgentWorkerId)} = v.agentworkerid,
    t.{Col(x => x.Priority)} = v.priority,
    t.{Col(x => x.NextPlanExecutionAt)} = v.nextplanexecutionat,
    t.{Col(x => x.ScheduledAt)} = v.scheduledat,
    t.{Col(x => x.MsgData)} = v.msgdata,
    t.{Col(x => x.Status)} = v.status,
    t.{Col(x => x.NumberOfFailures)} = v.numberoffailures,
    t.{Col(x => x.TimeoutTicks)} = v.timeoutticks,
    t.{Col(x => x.MaxNumberOfRetries)} = v.maxnumberofretries,
    t.{Col(x => x.SourceId)} = v.sourceid,
    t.{Col(x => x.PartitionLockId)} = v.partitionlockid,
    t.{Col(x => x.PartitionLockExpiresAt)} = v.partitionlockexpiresat,
    t.{Col(x => x.ProcessDeadline)} = v.processdeadline,
    t.{Col(x => x.ProcessStartedAt)} = v.processstartedat,
    t.{Col(x => x.FinalizedAt)} = v.finalizedat,
    t.{Col(x => x.WorkerLane)} = v.workerlane,
    t.{cVersion} = v.version,
    t.{Col(x => x.HostId)} = v.hostid,
    t.{Col(x => x.HostDisplayName)} = v.hostdisplayname
WHERE t.{Col(x => x.ClusterId)} = @ClusterId;";

        // Select t.id (not v.id): MySQL doesn't carry .NET type info for a parameter re-selected as a
        // literal column in a derived table, so Dapper's strict Guid parsing fails on v.id. t.id is a
        // real column with proper type metadata.
        var verifySql = $@"
SELECT t.{cId} FROM (
{derivedTable}
) AS v
JOIN {t} AS t ON t.{cId} = v.id AND t.{cVersion} = v.version
WHERE t.{Col(x => x.ClusterId)} = @ClusterId;";

        using var conn = await connManager.OpenAsync(connString, additionalConnConfig);
        using var trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            await conn.ExecuteAsync(updateSql, p, trans);
            var updatedIds = new HashSet<Guid>(await conn.QueryAsync<Guid>(verifySql, p, trans));
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

    protected override string BuildQueryIdsToLockSql(
        string whereSql,
        bool needsMetadataJoin,
        int countLimit,
        int offset,
        SortByCriteria? sortByCriteria)
    {
        // Push the application-level partition lock guard into the inner WHERE
        // so LIMIT applies only to rows that are actually acquirable.
        // Without this, already-locked rows can fill the LIMIT count and the outer
        // UPDATE silently discards them, producing a batch smaller than countLimit
        // and acquiring unnecessary DB row locks.
        // @LockNowUtc is provided by AcquireAndFetchAsync's parameter dictionary.
        var unlockedFilter = $" AND ({Col(x => x.PartitionLockId)} IS NULL " +
                             $"OR {Col(x => x.PartitionLockExpiresAt)} < @LockNowUtc)";
        var whereWithUnlocked = whereSql + unlockedFilter;

        var baseSql = base.BuildQueryIdsToLockSql(whereWithUnlocked, needsMetadataJoin, countLimit, offset, sortByCriteria);

        // MySQL doesn't support LIMIT inside IN subqueries directly.
        // Wrapping in a derived table is the standard workaround.
        // FOR UPDATE SKIP LOCKED inside the derived table gives the same race-free
        // candidate selection Postgres gets. Without it, concurrent workers can
        // SELECT the same candidate IDs and race on the UPDATE, leaving losers
        // with a silently smaller result set and causing downstream version
        // conflicts in fallback bucket assignment. Requires MySQL 8.0.1+.
        return $"SELECT {Col(x => x.Id)} FROM ({baseSql} FOR UPDATE SKIP LOCKED) AS _candidates";
    }

    private string BuildJobUpsertSql()
    {
        var t = TableName();
        var (cols, vals) = InsertColumnsAndParams();
        var cVersion = Col(x => x.Version);

        // MySQL note:
        // UpdateSetClauseWithoutVersion() writes every non-version column unconditionally
        // (just "col = @col"). If we only guard the Version column with IF(), a conflicting
        // upsert still overwrites Status, BucketId, PartitionLockId, etc. with stale values
        // while leaving Version untouched - and rowsAffected ends up 2 (not 0), because
        // those unguarded columns did change. That makes the conflict partially persist:
        // e.g. Status=InBucket + BucketId=<fallback> get written even though the caller
        // then throws JobMasterVersionConflictException and never pushes the job to the
        // fallback onboarding source. The job becomes orphaned in the fallback bucket.
        //
        // Fix: wrap every column assignment in the same IF() guard. On a version mismatch
        // no column changes, rowsAffected = 0, the check block below catches it and we
        // throw cleanly with the DB row fully unmodified.
        var versionMatch = $"({cVersion} = @ExpectedVersion OR (@ExpectedVersion IS NULL AND {cVersion} IS NULL))";
        var guardedSetClause = BuildGuardedUpdateSetClauseWithoutVersion(versionMatch);

        return $@"
INSERT INTO {t} ({cols}) VALUES ({vals})
ON DUPLICATE KEY UPDATE
    {guardedSetClause},
    {cVersion} = IF({versionMatch}, @Version, {cVersion});";
    }

    /// <summary>
    /// Wraps each "col = @param" pair returned by <see cref="SqlMasterJobsRepository.UpdateSetClauseWithoutVersion"/>
    /// in <c>IF(versionMatch, @param, col)</c> so the upsert becomes atomic w.r.t. the optimistic
    /// concurrency check: either every column is written (including Version) or none is.
    /// </summary>
    private string BuildGuardedUpdateSetClauseWithoutVersion(string versionMatch)
    {
        var baseClause = UpdateSetClauseWithoutVersion();
        var parts = baseClause.Split(new[] { ", " }, StringSplitOptions.None);
        var guarded = parts.Select(p =>
        {
            var eqIdx = p.IndexOf(" = ", StringComparison.Ordinal);
            if (eqIdx < 0)
                throw new InvalidOperationException(
                    $"Unexpected UpdateSetClauseWithoutVersion entry format: '{p}'. Expected 'col = @param'.");
            var col = p.Substring(0, eqIdx);
            var val = p.Substring(eqIdx + 3);
            return $"{col} = IF({versionMatch}, {val}, {col})";
        });
        return string.Join(", ", guarded);
    }

}