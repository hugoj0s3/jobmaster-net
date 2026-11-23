using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Extensions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using JobMaster.Sdk.Utils.Extensions;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
using JobMaster.SqlBase.Models.Jobs;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlServer.Master;

internal class SqlServerMasterJobsRepository : SqlMasterJobsRepository
{
    public SqlServerMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager,
        IKnownExceptionIdentifier knownExceptionIdentifier) : base(clusterConnectionConfig, connectionManager, knownExceptionIdentifier)
    {
    }

    public override string MasterRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;

    protected override string UpdateToLockTableHint => "WITH (UPDLOCK, READPAST)";

    // SQL Server-specific: one multi-row UPDATE ... FROM ... JOIN (VALUES ...) instead of N
    // sequential single-row UPDATEs. OUTPUT inserted.<id> gives back exactly which ids were
    // touched in the same round trip, so no follow-up query is needed here (unlike MySQL).
    //
    // Sorting the whole batch by Id (tried as a deadlock-avoidance measure) was confirmed
    // empirically NOT to help: Job.Id is a GUID v7 (time-ordered), so in a burst scenario a
    // batch's natural onboarding order already closely tracks Id order -- sorting was close to a
    // no-op, and benchmark deadlock counts with it were statistically the same as without it.
    //
    // Every call shuffles row order and adds a small jitter delay afterward (on every call, not
    // as a retry/fallback) to desynchronize concurrent buckets' lock-acquisition order and
    // request timing under burst load.
    public override async Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs)
    {
        if (jobs.Count == 0) return Array.Empty<JobRawModel>();

        var shuffled = jobs.ToList();
        JobMasterRandomUtil.Shuffle(shuffled);

        var result = await InternalBulkUpdateAsync(shuffled);
        await JitterDelayAsync();
        return result;
    }

    // Same reasoning as the BulkUpdateAsync(IList<JobRawModel>) override above: shuffle JobIds order
    // and add a small jitter delay afterward, on every call, to desynchronize concurrent buckets'
    // lock-acquisition order and request timing under burst load. This overload's UPDATE ... WHERE
    // Id IN (...) touches the same shared, GUID v7-keyed job table as the other overload.
    public override async Task BulkUpdateAsync(BulkJobUpdateRequest request)
    {
        if (request.JobIds.Count > 0)
        {
          JobMasterRandomUtil.Shuffle(request.JobIds);
        }
        
        await base.BulkUpdateAsync(request);
        await JitterDelayAsync();
    }

    // 10-25ms, inclusive -- small enough not to meaningfully slow down a single call, large enough
    // to spread out concurrent callers that would otherwise all retry/re-flush in lockstep.
    private static Task JitterDelayAsync() => Task.Delay(JobMasterRandomUtil.GetInt(10, 26));

    // Pure "attempt this batch" unit, no exception handling -- reusable at any fallback granularity.
    private async Task<IList<JobRawModel>> InternalBulkUpdateAsync(IList<JobRawModel> jobs)
    {
        var newVersions = jobs.ToDictionary(j => j.Id, _ => JobMasterRandomUtil.NewGuid4().ToString("N").ToLowerInvariant());

        var p = new DynamicParameters();
        p.Add("ClusterId", ClusterConnConfig.ClusterId, DbType.String);
        var valuesRows = new List<string>(jobs.Count);
        for (var i = 0; i < jobs.Count; i++)
        {
            var rec = SqlJobPersistenceConvertUtil.ToPersistence(jobs[i]);
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
UPDATE t WITH (UPDLOCK)
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
OUTPUT inserted.{cId}
FROM {t} AS t
JOIN (VALUES {string.Join(",\n", valuesRows)}) AS v(id, jobdefinitionid, triggersourcetype, bucketid,
    agentconnectionid, agentworkerid, priority, nextplanexecutionat, scheduledat, msgdata, status,
    numberoffailures, timeoutticks, maxnumberofretries, sourceid, partitionlockid, partitionlockexpiresat,
    processdeadline, processstartedat, finalizedat, workerlane, version, hostid, hostdisplayname, expectedversion)
    ON t.{cId} = v.id
WHERE t.{Col(x => x.ClusterId)} = @ClusterId
  AND t.{Col(x => x.Version)} = v.expectedversion;";

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
        var cClusterId = Col(x => x.ClusterId);
        var cId = Col(x => x.Id);
        var cVersion = Col(x => x.Version);

        return $@"
UPDATE {t} WITH (UPDLOCK, SERIALIZABLE)
SET {UpdateSetClause()}
WHERE {cClusterId} = @ClusterId
  AND {cId} = @Id
  AND ({cVersion} = @ExpectedVersion OR (@ExpectedVersion IS NULL AND {cVersion} IS NULL));

IF @@ROWCOUNT = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM {t} WITH (UPDLOCK, SERIALIZABLE) WHERE {cClusterId} = @ClusterId AND {cId} = @Id)
    BEGIN
        INSERT INTO {t} ({cols}) VALUES ({vals});
    END
END";
    }

}