using System.Data;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using Dapper;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Utils;
using JobMaster.SqlBase;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Extensions;
using JobMaster.SqlBase.Master;
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