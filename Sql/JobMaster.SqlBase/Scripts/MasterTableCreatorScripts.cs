using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

namespace JobMaster.SqlBase.Scripts;

internal static class MasterTableCreatorScripts
{
    public static string CreateGenericRecordTablesScript(ISqlGenerator sqlGenerator, string tablePrefix = "")
    {
       var scripts = new List<string>();
       
       // Default table (no suffix) - used by ClusterConfiguration
       scripts.Add(CreateGenericRecordEntry(sqlGenerator, tablePrefix, string.Empty));
       scripts.Add(CreateGenericRecordEntryValueTable(sqlGenerator, tablePrefix, string.Empty));
       
       // Family tables
       foreach (var suffix in GenericRecordSqlUtil.AllFamilySuffixes)
       {
           scripts.Add(CreateGenericRecordEntry(sqlGenerator, tablePrefix, suffix));
           scripts.Add(CreateGenericRecordEntryValueTable(sqlGenerator, tablePrefix, suffix));
       }
       
       return string.Join("\n", scripts);
    }

    public static IReadOnlyList<string> AllGenericRecordTableNames(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var names = new List<string>();
        var baseEntryName = sqlGenerator.TableNameFor<GenericRecordEntry>(tablePrefix);
        names.Add(baseEntryName);
        foreach (var suffix in GenericRecordSqlUtil.AllFamilySuffixes)
        {
            names.Add($"{baseEntryName}{suffix}");
        }
        return names;
    }

    public static string CreateRecurringScheduleTablesScript(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var tableName = sqlGenerator.TableNameFor<RecurringSchedule>(tablePrefix);
        
        var clusterIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.ClusterId);
        var clusterIdType = sqlGenerator.ColumnTypeFor<RecurringSchedulePersistenceRecord>(x => x.ClusterId, length: 250, nullable: false);
        
        var idCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.Id);
        var idType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: false);

        var expressionCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.Expression);
        var expressionType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: false);

        var expressionTypeIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.ExpressionTypeId);
        var expressionTypeIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);

        var jobDefinitionIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.JobDefinitionId);
        var jobDefinitionIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);

        var staticDefinitionIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.StaticDefinitionId);
        var staticDefinitionIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var staticDefinitionLastEnsuredCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.StaticDefinitionLastEnsured);
        var staticDefinitionLastEnsuredType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var profileIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.ProfileId);
        var profileIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 128, nullable: true);

        var statusCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.Status);
        var statusType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var recurringScheduleTypeCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.RecurringScheduleType);
        var recurringScheduleType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var dataCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.MsgData);
        var dataType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: false);

        // JSON-serialized metadata, read directly instead of LEFT JOINing the generic-record
        // entry/value tables -- see RecurringSchedulePersistenceRecord.MetadataJson.
        var metadataJsonCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.MetadataJson);
        var metadataJsonType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: true);

        var priorityCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.Priority);
        var priorityType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: true);

        var maxNumberOfRetriesCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.MaxNumberOfRetries);
        var maxNumberOfRetriesType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: true);

        var timeoutTicksCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.TimeoutTicks);
        var timeoutTicksType = sqlGenerator.ColumnTypeFor(typeof(long), nullable: true);

        var bucketIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.BucketId);
        var bucketIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var agentConnectionIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.AgentConnectionId);
        var agentConnectionIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var agentWorkerIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.AgentWorkerId);
        var agentWorkerIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var partitionLockIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.PartitionLockId);
        var partitionLockIdType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: true);

        var partitionLockExpiresAtCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.PartitionLockExpiresAt);
        var partitionLockExpiresAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var createdAtCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.CreatedAt);
        var createdAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);

        var startAfterCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.StartAfter);
        var startAfterType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var endBeforeCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.EndBefore);
        var endBeforeType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var lastPlanCoverageUntilCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.LastPlanCoverageUntil);
        var lastPlanCoverageUntilType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var lastExecutedPlanCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.LastExecutedPlan);
        var lastExecutedPlanType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var terminatedAtCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.TerminatedAt);
        var terminatedAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var hasFailedOnLastPlanExecutionCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.HasFailedOnLastPlanExecution);
        var hasFailedOnLastPlanExecutionType = sqlGenerator.ColumnTypeFor(typeof(bool), nullable: true);

        var isJobCancellationPendingCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.IsJobCancellationPending);
        var isJobCancellationPendingType = sqlGenerator.ColumnTypeFor(typeof(bool), nullable: true);
        
        var workerLaneCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.WorkerLane);
        var workerLaneType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var versionCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.Version);
        var versionType = sqlGenerator.ColumnTypeFor(typeof(string), length: 64, nullable: true);

        var hostIdCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.HostId);
        var hostIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var hostDisplayNameCol = sqlGenerator.ColumnNameFor<RecurringSchedulePersistenceRecord>(x => x.HostDisplayName);
        var hostDisplayNameType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var columns = new List<string>
        {
            $"{clusterIdCol} {clusterIdType}",
            $"{idCol} {idType}",
            $"{expressionCol} {expressionType}",
            $"{expressionTypeIdCol} {expressionTypeIdType}",
            $"{jobDefinitionIdCol} {jobDefinitionIdType}",
            $"{staticDefinitionIdCol} {staticDefinitionIdType}",
            $"{profileIdCol} {profileIdType}",
            $"{statusCol} {statusType}",
            $"{recurringScheduleTypeCol} {recurringScheduleType}",
            $"{staticDefinitionLastEnsuredCol} {staticDefinitionLastEnsuredType}",
            $"{dataCol} {dataType}",
            $"{metadataJsonCol} {metadataJsonType}",
            $"{priorityCol} {priorityType}",
            $"{maxNumberOfRetriesCol} {maxNumberOfRetriesType}",
            $"{timeoutTicksCol} {timeoutTicksType}",
            $"{bucketIdCol} {bucketIdType}",
            $"{agentConnectionIdCol} {agentConnectionIdType}",
            $"{agentWorkerIdCol} {agentWorkerIdType}",
            $"{partitionLockIdCol} {partitionLockIdType}",
            $"{hostIdCol} {hostIdType}",
            $"{hostDisplayNameCol} {hostDisplayNameType}",
            $"{partitionLockExpiresAtCol} {partitionLockExpiresAtType}",
            $"{createdAtCol} {createdAtType}",
            $"{startAfterCol} {startAfterType}",
            $"{endBeforeCol} {endBeforeType}",
            $"{terminatedAtCol} {terminatedAtType}",
            $"{lastPlanCoverageUntilCol} {lastPlanCoverageUntilType}",
            $"{lastExecutedPlanCol} {lastExecutedPlanType}",
            $"{hasFailedOnLastPlanExecutionCol} {hasFailedOnLastPlanExecutionType}",
            $"{isJobCancellationPendingCol} {isJobCancellationPendingType}",
            $"{workerLaneCol} {workerLaneType}",
            $"{versionCol} {versionType}"
        };

        var pkName = sqlGenerator.NormalizeIdentifierForDb($"pk_{tableName}recurring_schedule");
        var pk = $" CONSTRAINT {pkName} PRIMARY KEY ({clusterIdCol}, {idCol})";
        var create = $"CREATE TABLE {tableName} ({string.Join(", \n ", columns)}, \n {pk});";

        var indexes = new List<string>();
        // Hierarchical prefix strategy: cluster_id isolated, then cluster_id + status as base
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_id", (clusterIdCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status", (clusterIdCol, false, 250), (statusCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status_partition_lock", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (partitionLockIdCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status_partition_lock_expires", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (partitionLockIdCol, false, (int?)null), (partitionLockExpiresAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status_last_plan", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (lastPlanCoverageUntilCol, false, (int?)null)));
        // Extends last_plan index with partition lock columns so the acquire scan can filter already-locked
        // rows at the index level, avoiding a PK lookup per candidate row and reducing lock contention.
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status_last_plan_lock", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (lastPlanCoverageUntilCol, false, (int?)null), (partitionLockIdCol, false, (int?)null), (partitionLockExpiresAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status_start_after", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (startAfterCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status_end_before", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (endBeforeCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status_worker_lane", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (workerLaneCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status_terminated_at", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (terminatedAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_status_cancellation_pending", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (isJobCancellationPendingCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_partition_lock", (clusterIdCol, false, 250), (partitionLockIdCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_partition_lock_expires", (clusterIdCol, false, 250), (partitionLockIdCol, false, (int?)null), (partitionLockExpiresAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_last_plan", (clusterIdCol, false, 250), (lastPlanCoverageUntilCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_start_after", (clusterIdCol, false, 250), (startAfterCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_end_before", (clusterIdCol, false, 250), (endBeforeCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_worker_lane", (clusterIdCol, false, 250), (workerLaneCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_terminated_at", (clusterIdCol, false, 250), (terminatedAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_job_definition_id", (clusterIdCol, false, 250), (jobDefinitionIdCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_profile", (clusterIdCol, false, 250), (profileIdCol, false, 128)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_static_id", (clusterIdCol, false, 250), (staticDefinitionIdCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_expression_type", (clusterIdCol, false, 250), (expressionTypeIdCol, false, 250)));

        return $"{create}\n{string.Join("\n", indexes)}";
    }

    public static string CreateJobTablesScript(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var tableName = sqlGenerator.TableNameFor<Job>(tablePrefix);

        // Types
        var clusterIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.ClusterId);
        var clusterIdType = sqlGenerator.ColumnTypeFor<JobPersistenceRecord>(x => x.ClusterId, length: 250, nullable: false);

        var idCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.Id);
        var idType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: false);

        var jobDefinitionIdIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.JobDefinitionId);
        var jobDefinitionIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);

        var triggerSourceTypeCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.TriggerSourceType);
        var triggerSourceTypeType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var bucketIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.BucketId);
        var bucketIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var agentConnectionIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.AgentConnectionId);
        var agentConnectionIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var agentWorkerIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.AgentWorkerId);
        var agentWorkerIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var priorityCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.Priority);
        var priorityType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var scheduledAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.ScheduledAt);
        var scheduledAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);

        var nextPlanExecutionAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.NextPlanExecutionAt);
        var nextPlanExecutionAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var dataCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.MsgData);
        var dataType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: false);

        // JSON-serialized metadata, read directly instead of LEFT JOINing the generic-record
        // entry/value tables -- see JobPersistenceRecord.MetadataJson.
        var metadataJsonCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.MetadataJson);
        var metadataJsonType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: true);

        var statusCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.Status);
        var statusType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var numberOfFailuresCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.NumberOfFailures);
        var numberOfFailuresType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var timeoutTicksCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.TimeoutTicks);
        var timeoutTicksType = sqlGenerator.ColumnTypeFor(typeof(long), nullable: false);

        var maxRetriesCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.MaxNumberOfRetries);
        var maxRetriesType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var createdAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.CreatedAt);
        var createdAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);

        var sourceIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.SourceId);
        var sourceIdType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: true);

        var partitionLockIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.PartitionLockId);
        var partitionLockIdType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: true);

        var partitionLockExpiresAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.PartitionLockExpiresAt);
        var partitionLockExpiresAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var processDeadlineCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.ProcessDeadline);
        var processDeadlineType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var processStartedAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.ProcessStartedAt);
        var processStartedAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var finalizedAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.FinalizedAt);
        var finalizedAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);
        
        var workerLaneCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.WorkerLane);
        var workerLaneType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);
        
        var versionCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.Version);
        var versionType = sqlGenerator.ColumnTypeFor(typeof(string), length: 64, nullable: true);
        
        var hostIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.HostId);
        var hostIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var hostDisplayNameCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.HostDisplayName);
        var hostDisplayNameType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var columns = new List<string>
        {
            $"{clusterIdCol} {clusterIdType}",
            $"{idCol} {idType}",
            $"{jobDefinitionIdIdCol} {jobDefinitionIdType}",
            $"{triggerSourceTypeCol} {triggerSourceTypeType}",
            $"{bucketIdCol} {bucketIdType}",
            $"{agentConnectionIdCol} {agentConnectionIdType}",
            $"{agentWorkerIdCol} {agentWorkerIdType}",
            $"{priorityCol} {priorityType}",
            $"{scheduledAtCol} {scheduledAtType}",
            $"{nextPlanExecutionAtCol} {nextPlanExecutionAtType}",
            $"{dataCol} {dataType}",
            $"{metadataJsonCol} {metadataJsonType}",
            $"{statusCol} {statusType}",
            $"{numberOfFailuresCol} {numberOfFailuresType}",
            $"{timeoutTicksCol} {timeoutTicksType}",
            $"{maxRetriesCol} {maxRetriesType}",
            $"{createdAtCol} {createdAtType}",
            $"{sourceIdCol} {sourceIdType}",
            $"{partitionLockIdCol} {partitionLockIdType}",
            $"{partitionLockExpiresAtCol} {partitionLockExpiresAtType}",
            $"{processDeadlineCol} {processDeadlineType}",
            $"{processStartedAtCol} {processStartedAtType}",
            $"{finalizedAtCol} {finalizedAtType}",
            $"{workerLaneCol} {workerLaneType}",
            $"{versionCol} {versionType}",
            $"{hostIdCol} {hostIdType}",
            $"{hostDisplayNameCol} {hostDisplayNameType}"
        };

        var pk = $" CONSTRAINT pk_{tableName}job PRIMARY KEY ({clusterIdCol}, {idCol})";
        pk = $" CONSTRAINT {sqlGenerator.NormalizeIdentifierForDb($"pk_{tableName}job")} PRIMARY KEY ({clusterIdCol}, {idCol})";
        var create = $"CREATE TABLE {tableName} ({string.Join(", \n ", columns)}, \n {pk});";

        var indexes = new List<string>();
        // Hierarchical prefix strategy: cluster_id isolated, then cluster_id + status as base
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_id", (clusterIdCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_status", (clusterIdCol, false, 250), (statusCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_status_next_plan", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (nextPlanExecutionAtCol, false, (int?)null)));
        // Extends the next_plan index with partition lock columns so the acquire scan can filter already-locked
        // rows at the index level, avoiding a PK lookup per candidate row and reducing lock contention.
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_status_next_plan_lock", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (nextPlanExecutionAtCol, false, (int?)null), (partitionLockIdCol, false, (int?)null), (partitionLockExpiresAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_status_process_deadline", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (processDeadlineCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_status_partition_lock", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (partitionLockIdCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_status_partition_lock_expires", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (partitionLockIdCol, false, (int?)null), (partitionLockExpiresAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_status_worker_lane", (clusterIdCol, false, 250), (statusCol, false, (int?)null), (workerLaneCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_next_plan", (clusterIdCol, false, 250), (nextPlanExecutionAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_process_deadline", (clusterIdCol, false, 250), (processDeadlineCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_partition_lock", (clusterIdCol, false, 250), (partitionLockIdCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_partition_lock_expires", (clusterIdCol, false, 250), (partitionLockIdCol, false, (int?)null), (partitionLockExpiresAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_worker_lane", (clusterIdCol, false, 250), (workerLaneCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_job_definition", (clusterIdCol, false, 250), (jobDefinitionIdIdCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_cluster_source_id", (clusterIdCol, false, 250), (sourceIdCol, false, (int?)null)));

        return $"{create}\n{string.Join("\n", indexes)}";
    }

    public static string CreateJobExecutionTableScript(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
        var tableName = $"{prefix}job_execution";

        var clusterIdCol = "cluster_id";
        var clusterIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);

        var idCol = "id";
        var idType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: false);

        var jobIdCol = "job_id";
        var jobIdType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: false);

        var startedAtCol = "started_at";
        var startedAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);

        var agentConnectionIdCol = "agent_connection_id";
        var agentConnectionIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var agentWorkerIdCol = "agent_worker_id";
        var agentWorkerIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var bucketIdCol = "bucket_id";
        var bucketIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var hostIdCol = "host_id";
        var hostIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var hostDisplayNameCol = "host_display_name";
        var hostDisplayNameType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var finalizedAtCol = "finalized_at";
        var finalizedAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var outcomeMessageCol = "outcome_message";
        var outcomeMessageType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: true);

        var outcomeCol = "outcome";
        var outcomeType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var columns = new List<string>
        {
            $"{clusterIdCol} {clusterIdType}",
            $"{idCol} {idType}",
            $"{jobIdCol} {jobIdType}",
            $"{startedAtCol} {startedAtType}",
            $"{agentConnectionIdCol} {agentConnectionIdType}",
            $"{agentWorkerIdCol} {agentWorkerIdType}",
            $"{bucketIdCol} {bucketIdType}",
            $"{hostIdCol} {hostIdType}",
            $"{hostDisplayNameCol} {hostDisplayNameType}",
            $"{finalizedAtCol} {finalizedAtType}",
            $"{outcomeMessageCol} {outcomeMessageType}",
            $"{outcomeCol} {outcomeType}",
        };

        var pkName = sqlGenerator.NormalizeIdentifierForDb($"pk_{tableName}");
        var pk = $" CONSTRAINT {pkName} PRIMARY KEY ({clusterIdCol}, {idCol})";
        var create = $"CREATE TABLE {tableName} ({string.Join(", \n ", columns)}, \n {pk});";

        var indexes = new List<string>
        {
            sqlGenerator.CreateIndex(tableName, $"idx_{tableName}_cluster_job_id",
                (clusterIdCol, false, 250), (jobIdCol, false, null)),
            sqlGenerator.CreateIndex(tableName, $"idx_{tableName}_cluster_id",
                (clusterIdCol, false, 250))
        };

        return $"{create}\n{string.Join("\n", indexes)}";
    }

    public static string CreateLogTableScript(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
        var tableName = $"{prefix}log";

        var clusterIdCol = "cluster_id";
        var clusterIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);

        var idCol = "id";
        var idType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: false);

        var levelCol = "level";
        var levelType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var messageCol = "message";
        var messageType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: false);

        var categoryCol = "category";
        var categoryType = sqlGenerator.ColumnTypeFor(typeof(string), length: 100, nullable: true);

        var referenceIdCol = "reference_id";
        var referenceIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var timestampUtcCol = "timestamp_utc";
        var timestampUtcType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);

        var hostCol = "host";
        var hostType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var sourceMemberCol = "source_member";
        var sourceMemberType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var sourceFileCol = "source_file";
        var sourceFileType = sqlGenerator.ColumnTypeFor(typeof(string), length: 500, nullable: true);

        var sourceLineCol = "source_line";
        var sourceLineType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: true);

        var columns = new List<string>
        {
            $"{clusterIdCol} {clusterIdType}",
            $"{idCol} {idType}",
            $"{levelCol} {levelType}",
            $"{messageCol} {messageType}",
            $"{categoryCol} {categoryType}",
            $"{referenceIdCol} {referenceIdType}",
            $"{timestampUtcCol} {timestampUtcType}",
            $"{hostCol} {hostType}",
            $"{sourceMemberCol} {sourceMemberType}",
            $"{sourceFileCol} {sourceFileType}",
            $"{sourceLineCol} {sourceLineType}",
        };

        var pkName = sqlGenerator.NormalizeIdentifierForDb($"pk_{tableName}");
        var pk = $" CONSTRAINT {pkName} PRIMARY KEY ({clusterIdCol}, {idCol})";
        var create = $"CREATE TABLE {tableName} ({string.Join(", \n ", columns)}, \n {pk});";

        var indexes = new List<string>
        {
            sqlGenerator.CreateIndex(tableName, $"idx_{tableName}_cluster_timestamp",
                (clusterIdCol, false, 250), (timestampUtcCol, false, (int?)null)),
            sqlGenerator.CreateIndex(tableName, $"idx_{tableName}_cluster_level_timestamp",
                (clusterIdCol, false, 250), (levelCol, false, (int?)null), (timestampUtcCol, false, (int?)null)),
            sqlGenerator.CreateIndex(tableName, $"idx_{tableName}_cluster_category",
                (clusterIdCol, false, 250), (categoryCol, false, 100), (referenceIdCol, false, 250)),
        };

        return $"{create}\n{string.Join("\n", indexes)}";
    }

    private static string CreateGenericRecordEntry(ISqlGenerator sqlGenerator, string tablePrefix, string familySuffix)
    {
       var baseTableName = sqlGenerator.TableNameFor<GenericRecordEntry>(tablePrefix);
       var tableName = string.IsNullOrEmpty(familySuffix) ? baseTableName : $"{baseTableName}{familySuffix}";
       
        var recordIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.RecordUniqueId);
        var recordIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.RecordUniqueId, length: 450, nullable: false);
       
        var clusterIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.ClusterId);
        var clusterIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.ClusterId, length: 100, nullable: false);
       
        var groupIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.GroupId);
        var groupIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.GroupId, length: 100, nullable: false);
       
        var entryIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.EntryId);
        var entryIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.EntryId, length: 250, nullable: false);

        var createdAtCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.CreatedAt);
        var createdAtType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.CreatedAt, nullable: false);
       
        var expiresAtCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.ExpiresAt);
        var expiresAtType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.ExpiresAt, nullable: true);
        
        var entryIdGuidCol = sqlGenerator.ColumnNameFor<SqlGenericRecordEntry>(x => x.EntryIdGuid);
        var entryIdGuidType = sqlGenerator.ColumnTypeFor<SqlGenericRecordEntry>(x => x.EntryIdGuid, nullable: true);

        var columns = new List<string>();
        columns.Add($"{recordIdCol} {recordIdType} PRIMARY KEY");
        columns.Add($"{clusterIdCol} {clusterIdType} ");
        columns.Add($"{groupIdCol} {groupIdType}");
        columns.Add($"{entryIdCol} {entryIdType}");
        columns.Add($"{createdAtCol} {createdAtType}");
        columns.Add($"{expiresAtCol} {expiresAtType}");
        columns.Add($"{entryIdGuidCol} {entryIdGuidType}");

        var createTableScript = $"CREATE TABLE {tableName} ({string.Join(", \n", columns)} );";

        var indexScripts = new List<string>();
        // Hierarchical prefix strategy: each index extends the base (cluster_id + group_id)
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_id", (clusterIdCol, false, 100)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_group", (clusterIdCol, false, 100), (groupIdCol, false, 100)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_group_ready", (clusterIdCol, false, 100), (groupIdCol, false, 100)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_group_ready_entry", (clusterIdCol, false, 100), (groupIdCol, false, 100), (entryIdCol, false, 250)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_group_ready_expires_at", (clusterIdCol, false, 100), (groupIdCol, false, 100), (expiresAtCol, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_group_ready_created_at", (clusterIdCol, false, 100), (groupIdCol, false, 100), (createdAtCol, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_group_ready_entry_guid", (clusterIdCol, false, 100), (groupIdCol, false, 100), (entryIdGuidCol, false, (int?)null)));
        var uniqueIdxName = sqlGenerator.NormalizeIdentifierForDb($"idx_{tableName}_unique");
        indexScripts.Add($"CREATE UNIQUE INDEX {uniqueIdxName} ON {tableName} ({clusterIdCol}, {groupIdCol}, {entryIdCol});");
        
        createTableScript = $"{createTableScript}\n{string.Join("\n", indexScripts)}";
        
        return createTableScript;
    }

    public static string CreateDistributedLockTablesScript(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var prefix = tablePrefix == string.Empty ? string.Empty : tablePrefix;
        var tableName = $"{prefix}distributed_lock";

        // Reuse types consistent with existing models for cluster_id
        var clusterIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.ClusterId);
        var clusterIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.ClusterId, length: 250, nullable: false);

        var lockKeyCol = "lock_key";
        var lockKeyType = sqlGenerator.ColumnTypeFor(typeof(string), length: 450, nullable: false);

        var expiresAtCol = "expires_at";
        var expiresAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);

        var lockTokenCol = "lock_token";
        var lockTokenType = sqlGenerator.ColumnTypeFor(typeof(string), length: 64, nullable: false);

        var columns = new List<string>();
        columns.Add($"{clusterIdCol} {clusterIdType}");
        columns.Add($"{lockKeyCol} {lockKeyType}");
        columns.Add($"{expiresAtCol} {expiresAtType}");
        columns.Add($"{lockTokenCol} {lockTokenType}");

        var pk = $" CONSTRAINT pk_{tableName}distributed_lock PRIMARY KEY ({clusterIdCol}, {lockKeyCol})";
        pk = $" CONSTRAINT {sqlGenerator.NormalizeIdentifierForDb($"pk_{tableName}distributed_lock")} PRIMARY KEY ({clusterIdCol}, {lockKeyCol})";
        var create = $"CREATE TABLE {tableName} ({string.Join(", \n ", columns)}, \n {pk}) ;";

        var indexes = new List<string>();
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_expires_at", (expiresAtCol, false, (int?)null)));

        return $"{create}\n{string.Join("\n", indexes)}";
    }

    private static string CreateGenericRecordEntryValueTable(ISqlGenerator sqlGenerator, string tablePrefix, string familySuffix)
    {
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
       
         var baseValueTableName = $"{prefix}generic_record_entry_value";
         var tableName = string.IsNullOrEmpty(familySuffix) ? baseValueTableName : $"{baseValueTableName}{familySuffix}";

         var recordIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.RecordUniqueId);
         var recordIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.RecordUniqueId, length: 450, nullable: false);
         
         var keyNameCol = "key_name";
         var keyNameType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);
      
        var valueTextCol = "value_text";
        var valueTextType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: true);
       
        var valueBinaryCol = "value_binary";
        var valueBinaryType = sqlGenerator.ColumnTypeFor(typeof(byte[]), nullable: true);

        var valueInt64 = "value_int64";
        var valueInt64Type = sqlGenerator.ColumnTypeFor(typeof(long), nullable: true);
       
        var valueBool = "value_bool";
        var valueBoolType = sqlGenerator.ColumnTypeFor(typeof(bool), nullable: true);
       
        var valueDecimal = "value_decimal";
        var valueDecimalType = sqlGenerator.ColumnTypeFor(typeof(decimal), nullable: true, precision: 38, scale: 10);
       
        var valueDateTime = "value_date_time";
        var valueDateTimeType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);
        
        var valueGuid = "value_guid";
        var valueGuidType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: true);
       
        var columns = new List<string>();
        columns.Add($"{recordIdCol} {recordIdType}");
        columns.Add($"{keyNameCol} {keyNameType}");
        columns.Add($"{valueTextCol} {valueTextType}"); 
        columns.Add($"{valueBinaryCol} {valueBinaryType}");
        columns.Add($"{valueInt64} {valueInt64Type}");
        columns.Add($"{valueBool} {valueBoolType}");
        columns.Add($"{valueDecimal} {valueDecimalType}");
        columns.Add($"{valueDateTime} {valueDateTimeType}");
        columns.Add($"{valueGuid} {valueGuidType}");
        
        var sfx = string.IsNullOrEmpty(familySuffix) ? "" : familySuffix;
        var primaryKeyName = sqlGenerator.NormalizeIdentifierForDb($"pk_{tablePrefix}generic_record_entry_value{sfx}");
        var foreignKeyName = sqlGenerator.NormalizeIdentifierForDb($"fk_{tablePrefix}generic_record_entry_value{sfx}");
        var primaryKey = $" CONSTRAINT {primaryKeyName} PRIMARY KEY ({recordIdCol}, {keyNameCol})";
        var entryTableName = string.IsNullOrEmpty(familySuffix) ? sqlGenerator.TableNameFor<GenericRecordEntry>(tablePrefix) : $"{sqlGenerator.TableNameFor<GenericRecordEntry>(tablePrefix)}{familySuffix}";
        var foreignKey = $" CONSTRAINT {foreignKeyName} FOREIGN KEY ({recordIdCol}) REFERENCES {entryTableName} ({recordIdCol})";
        var createTableScript = $"CREATE TABLE {tableName} ({string.Join(", \n", columns)}, \n {primaryKey},\n {foreignKey}); \n";
        
        var indexScripts = new List<string>();
        var indexPrefix = $"idx_{tableName}";
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_value_text", (keyNameCol, false, 250), (valueTextCol, true, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_value_binary", (keyNameCol, false, 250), (valueBinaryCol, true, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_value_int64", (keyNameCol, false, 250), (valueInt64, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_value_bool", (keyNameCol, false, 250), (valueBool, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_value_decimal", (keyNameCol, false, 250), (valueDecimal, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_value_datetime", (keyNameCol, false, 250), (valueDateTime, false, (int?)null)));

        // Composite indexes for common EXISTS filters:
        //   ... WHERE v2.record_unique_id = e.record_unique_id AND v2.key_name = @Key AND v2.value_* = @Value
        // KeyName + Value + RecordUniqueId supports filtering + join without scanning.
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_key_int64_record", (keyNameCol, false, 250), (valueInt64, false, (int?)null), (recordIdCol, false, 450)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_key_bool_record", (keyNameCol, false, 250), (valueBool, false, (int?)null), (recordIdCol, false, 450)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_key_decimal_record", (keyNameCol, false, 250), (valueDecimal, false, (int?)null), (recordIdCol, false, 450)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_key_datetime_record", (keyNameCol, false, 250), (valueDateTime, false, (int?)null), (recordIdCol, false, 450)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"{indexPrefix}_key_guid_record", (keyNameCol, false, 250), (valueGuid, false, (int?)null), (recordIdCol, false, 450)));

        createTableScript = $"{createTableScript}\n{string.Join("\n", indexScripts)}";

        return createTableScript;
    }
}