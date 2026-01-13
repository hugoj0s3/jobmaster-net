using JobMaster.Sdk.Contracts.Jobs;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Models.Jobs;
using JobMaster.Sdk.Contracts.Models.RecurringSchedules;

namespace JobMaster.Sql.Scripts;

public static class MasterTableCreatorScripts
{
    public static string CreateGenericRecordTablesScript(ISqlGenerator sqlGenerator, string tablePrefix = "")
    {
       var script1 = CreateGenericRecordEntry(sqlGenerator, tablePrefix);
       var script2 = CreateGenericRecordEntryValueTable(sqlGenerator, tablePrefix);
       
       return $"{script1}\n{script2}";
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
        var partitionLockIdType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: true);

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
            $"{priorityCol} {priorityType}",
            $"{maxNumberOfRetriesCol} {maxNumberOfRetriesType}",
            $"{timeoutTicksCol} {timeoutTicksType}",
            $"{bucketIdCol} {bucketIdType}",
            $"{agentConnectionIdCol} {agentConnectionIdType}",
            $"{agentWorkerIdCol} {agentWorkerIdType}",
            $"{partitionLockIdCol} {partitionLockIdType}",
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
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_status", (statusCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_status_deactivated_at", (statusCol, false, (int?)null), (terminatedAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_job_definition_id", (jobDefinitionIdCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_expression_type_id", (expressionTypeIdCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_partition_lock_id", (partitionLockIdCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_last_plan_coverage_until", (lastPlanCoverageUntilCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_start_after", (startAfterCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_end_before", (endBeforeCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_created_at", (createdAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_partition_lock_expires_at", (partitionLockExpiresAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_is_job_cancellation_pending", (isJobCancellationPendingCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_profile", (clusterIdCol, false, 250), (profileIdCol, false, 128)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_static_id", (clusterIdCol, false, 250), (staticDefinitionIdCol, false, 250)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_worker_lane", (workerLaneCol, false, 250)));

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

        var scheduledTypeCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.ScheduledType);
        var scheduledTypeType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var bucketIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.BucketId);
        var bucketIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var agentConnectionIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.AgentConnectionId);
        var agentConnectionIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var agentWorkerIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.AgentWorkerId);
        var agentWorkerIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);

        var priorityCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.Priority);
        var priorityType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: false);

        var originalScheduledAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.OriginalScheduledAt);
        var originalScheduledAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);

        var scheduledAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.ScheduledAt);
        var scheduledAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);

        var dataCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.MsgData);
        var dataType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: false);

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

        var recurringScheduleIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.RecurringScheduleId);
        var recurringScheduleIdType = sqlGenerator.ColumnTypeFor(typeof(Guid), nullable: true);

        var partitionLockIdCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.PartitionLockId);
        var partitionLockIdType = sqlGenerator.ColumnTypeFor(typeof(int), nullable: true);

        var partitionLockExpiresAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.PartitionLockExpiresAt);
        var partitionLockExpiresAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var processDeadlineCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.ProcessDeadline);
        var processDeadlineType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var processingStartedAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.ProcessingStartedAt);
        var processingStartedAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);

        var succeedExecutedAtCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.SucceedExecutedAt);
        var succeedExecutedAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);
        
        var workerLaneCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.WorkerLane);
        var workerLaneType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: true);
        
        var versionCol = sqlGenerator.ColumnNameFor<JobPersistenceRecord>(x => x.Version);
        var versionType = sqlGenerator.ColumnTypeFor(typeof(string), length: 64, nullable: true);

        var columns = new List<string>
        {
            $"{clusterIdCol} {clusterIdType}",
            $"{idCol} {idType}",
            $"{jobDefinitionIdIdCol} {jobDefinitionIdType}",
            $"{scheduledTypeCol} {scheduledTypeType}",
            $"{bucketIdCol} {bucketIdType}",
            $"{agentConnectionIdCol} {agentConnectionIdType}",
            $"{agentWorkerIdCol} {agentWorkerIdType}",
            $"{priorityCol} {priorityType}",
            $"{originalScheduledAtCol} {originalScheduledAtType}",
            $"{scheduledAtCol} {scheduledAtType}",
            $"{dataCol} {dataType}",
            $"{statusCol} {statusType}",
            $"{numberOfFailuresCol} {numberOfFailuresType}",
            $"{timeoutTicksCol} {timeoutTicksType}",
            $"{maxRetriesCol} {maxRetriesType}",
            $"{createdAtCol} {createdAtType}",
            $"{recurringScheduleIdCol} {recurringScheduleIdType}",
            $"{partitionLockIdCol} {partitionLockIdType}",
            $"{partitionLockExpiresAtCol} {partitionLockExpiresAtType}",
            $"{processDeadlineCol} {processDeadlineType}",
            $"{processingStartedAtCol} {processingStartedAtType}",
            $"{succeedExecutedAtCol} {succeedExecutedAtType}",
            $"{workerLaneCol} {workerLaneType}",
            $"{versionCol} {versionType}"
        };

        var pk = $" CONSTRAINT pk_{tableName}job PRIMARY KEY ({clusterIdCol}, {idCol})";
        pk = $" CONSTRAINT {sqlGenerator.NormalizeIdentifierForDb($"pk_{tableName}job")} PRIMARY KEY ({clusterIdCol}, {idCol})";
        var create = $"CREATE TABLE {tableName} ({string.Join(", \n ", columns)}, \n {pk});";

        var indexes = new List<string>();
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_scheduled_at", (scheduledAtCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_status", (statusCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_process_deadline", (processDeadlineCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_partition_lock_id", (partitionLockIdCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_worker_lane", (workerLaneCol, false, (int?)null)));
        indexes.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}job_recurring_schedule_id", (recurringScheduleIdCol, false, (int?)null)));

        return $"{create}\n{string.Join("\n", indexes)}";
    }

    private static string CreateGenericRecordEntry(ISqlGenerator sqlGenerator, string tablePrefix)
    {
       var tableName = sqlGenerator.TableNameFor<GenericRecordEntry>(tablePrefix);
       
        var recordIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.RecordUniqueId);
        var recordIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.RecordUniqueId, length: 450, nullable: false);
       
        var clusterIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.ClusterId);
        var clusterIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.ClusterId, length: 100, nullable: false);
       
        var groupIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.GroupId);
        var groupIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.GroupId, length: 100, nullable: false);
       
        var entryIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.EntryId);
        var entryIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.EntryId, length: 250, nullable: false);
       
        var subjectTypeCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.SubjectType);
        var subjectTypeType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.SubjectType, length: 100, nullable: true);
        
        var subjectIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.SubjectId);
        var subjectIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.SubjectId, length: 250, nullable: true);
       
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
        columns.Add($"{subjectTypeCol} {subjectTypeType}");
        columns.Add($"{subjectIdCol} {subjectIdType}");
        columns.Add($"{createdAtCol} {createdAtType}");
        columns.Add($"{expiresAtCol} {expiresAtType}");
        columns.Add($"{entryIdGuidCol} {entryIdGuidType}");
       
        var createTableScript = $"CREATE TABLE {tableName} ({string.Join(", \n", columns)} );";
        
        var indexScripts = new List<string>();
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_cluster_id", (clusterIdCol, false, 100)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_group_id", (groupIdCol, false, 100)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_entry_id", (entryIdCol, false, 250)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_subject_type", (subjectTypeCol, false, 100)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_subject_id", (subjectIdCol, false, 250)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_expires_at", (expiresAtCol, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_created_at", (createdAtCol, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_entry_id_guid", (entryIdGuidCol, false, (int?)null)));
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

    private static string CreateGenericRecordEntryValueTable(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
       
         var tableName = $"{prefix}generic_record_entry_value";

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
        
        var primaryKeyName = sqlGenerator.NormalizeIdentifierForDb($"pk_{tablePrefix}generic_record_entry_value");
        var foreignKeyName = sqlGenerator.NormalizeIdentifierForDb($"fk_{tablePrefix}generic_record_entry_value");
        var primaryKey = $" CONSTRAINT {primaryKeyName} PRIMARY KEY ({recordIdCol}, {keyNameCol})";
        var foreignKey = $" CONSTRAINT {foreignKeyName} FOREIGN KEY ({recordIdCol}) REFERENCES {sqlGenerator.TableNameFor<GenericRecordEntry>(tablePrefix)} ({recordIdCol})";
        var createTableScript = $"CREATE TABLE {tableName} ({string.Join(", \n", columns)}, \n {primaryKey},\n {foreignKey}); \n";
        
        var indexScripts = new List<string>();
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_value_text", (keyNameCol, false, 250), (valueTextCol, true, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_value_binary", (keyNameCol, false, 250), (valueBinaryCol, true, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_value_int64", (keyNameCol, false, 250), (valueInt64, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_value_bool", (keyNameCol, false, 250), (valueBool, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_value_decimal", (keyNameCol, false, 250), (valueDecimal, false, (int?)null)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_value_datetime", (keyNameCol, false, 250), (valueDateTime, false, (int?)null)));

        // Composite indexes for common EXISTS filters:
        //   ... WHERE v2.record_unique_id = e.record_unique_id AND v2.key_name = @Key AND v2.value_* = @Value
        // KeyName + Value + RecordUniqueId supports filtering + join without scanning.
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_key_int64_record", (keyNameCol, false, 250), (valueInt64, false, (int?)null), (recordIdCol, false, 450)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_key_bool_record", (keyNameCol, false, 250), (valueBool, false, (int?)null), (recordIdCol, false, 450)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_key_decimal_record", (keyNameCol, false, 250), (valueDecimal, false, (int?)null), (recordIdCol, false, 450)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_key_datetime_record", (keyNameCol, false, 250), (valueDateTime, false, (int?)null), (recordIdCol, false, 450)));
        indexScripts.Add(sqlGenerator.CreateIndex($"{tableName}", $"idx_{tablePrefix}generic_record_entry_value_key_guid_record", (keyNameCol, false, 250), (valueGuid, false, (int?)null), (recordIdCol, false, 450)));

        createTableScript = $"{createTableScript}\n{string.Join("\n", indexScripts)}";

        return createTableScript;
    }
}