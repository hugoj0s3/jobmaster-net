using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.SqlBase.Scripts;

internal class AgentTableCreatorScripts
{
    public const string LeasedUntilColumn = "leased_until";
    public const string LeaseCountColumn = "lease_count";

    public static string CreateBucketDispatcherTableScript(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var prefix = tablePrefix == string.Empty ? string.Empty : tablePrefix;
        var tableName = $"{prefix}bucket_dispatcher";

        var bucketCol = "bucket_address_id";
        var bucketType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);

        var columns = new List<string>
        {
            $"{bucketCol} {bucketType} PRIMARY KEY"
        };

        var create = $"CREATE TABLE {tableName} ({string.Join(", ", columns)});";
        return create;
    }
    
    public static string CreateMessageDispatcherTableScript(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var prefix = tablePrefix == string.Empty ? string.Empty : tablePrefix;
        var tableName = $"{prefix}message_dispatcher";
        var bucketTable = $"{prefix}bucket_dispatcher";

        // Column names (snake_case to align with repository queries)
        var msgIdCol = sqlGenerator.ColumnNameFor<JobMasterRawMessage>(x => x.MessageId);
        var payloadCol = sqlGenerator.ColumnNameFor<JobMasterRawMessage>(x => x.Payload);
        var refTimeCol = sqlGenerator.ColumnNameFor<JobMasterRawMessage>(x => x.ReferenceTime);
        var corrIdCol = sqlGenerator.ColumnNameFor<JobMasterRawMessage>(x => x.CorrelationId);
        var enqAtCol = sqlGenerator.ColumnNameFor<JobMasterRawMessage>(x => x.EnqueuedAt);
        var bucketCol = "bucket_address_id";
        // Set while leased (UTC, database clock); null or past = available.
        var leasedUntilCol = LeasedUntilColumn;
        // Times the message was leased (incremented by each lease).
        var leaseCountCol = LeaseCountColumn;

        // Types (provider decides exact SQL types)
        // MessageId is now an application-generated GUID string
        var msgIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 64, nullable: false);
        var payloadType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: false);
        var refTimeType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);
        var corrIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);
        var enqAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);
        var bucketType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);
        var leasedUntilType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);
        var leaseCountType = LeaseCountType(sqlGenerator);

        var cols = new List<string>
        {
            $"{msgIdCol} {msgIdType}",
            $"{bucketCol} {bucketType}",
            $"{payloadCol} {payloadType}",
            $"{refTimeCol} {refTimeType}",
            $"{corrIdCol} {corrIdType}",
            $"{enqAtCol} {enqAtType}",
            $"{leasedUntilCol} {leasedUntilType}",
            $"{leaseCountCol} {leaseCountType}"
        };

        var pkName = sqlGenerator.NormalizeIdentifierForDb($"pk_{tableName}message_dispatcher");
        var fkName = sqlGenerator.NormalizeIdentifierForDb($"fk_{tableName}message_dispatcher_bucket");
        var pk = $" CONSTRAINT {pkName} PRIMARY KEY ({msgIdCol})";
        var fk = $" CONSTRAINT {fkName} FOREIGN KEY ({bucketCol}) REFERENCES {bucketTable}({bucketCol})";

        var create = $"CREATE TABLE {tableName} ({string.Join(", ", cols)},{pk},{fk});";

        // Hierarchical prefix strategy: bucket isolated, then bucket + ref_time as base
        var idxBucket = sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_bucket", (bucketCol, false, 250));
        var idxBucketRef = sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_bucket_ref", (bucketCol, false, 250), (refTimeCol, false, null));
        // Covers full ORDER BY (ref_time ASC, message_id ASC) without filesort
        var idxBucketRefMsg = sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_bucket_ref_msg", (bucketCol, false, 250), (refTimeCol, false, null), (msgIdCol, false, 64));
        // Covers cleanup/stale message queries by enqueue age
        var idxBucketEnqAt = sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_bucket_enq_at", (bucketCol, false, 250), (enqAtCol, false, null));

        return $"{create}\n{idxBucket}\n{idxBucketRef}\n{idxBucketRefMsg}\n{idxBucketEnqAt}";
    }

    public static string AddLeasedUntilColumnScript(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var tableName = $"{tablePrefix}message_dispatcher";
        var leasedUntilType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: true);
        return $"ALTER TABLE {tableName} ADD {LeasedUntilColumn} {leasedUntilType};";
    }

    public static string AddLeaseCountColumnScript(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var tableName = $"{tablePrefix}message_dispatcher";
        return $"ALTER TABLE {tableName} ADD {LeaseCountColumn} {LeaseCountType(sqlGenerator)};";
    }

    // NOT NULL DEFAULT 0: existing rows (and pushes, which don't set it) start at 0.
    private static string LeaseCountType(ISqlGenerator sqlGenerator)
        => $"{sqlGenerator.ColumnTypeFor(typeof(int), nullable: false)} DEFAULT 0";

    public static string CreateAgentConnectionFingerprint(ISqlGenerator sqlGenerator, string tablePrefix)
    {
        var prefix = tablePrefix == string.Empty ? string.Empty : tablePrefix;
        var tableName = $"{prefix}agent_conn_fingerprint";

        // Reuse cluster_id type from GenericRecordEntry for consistency
        var clusterIdCol = sqlGenerator.ColumnNameFor<GenericRecordEntry>(x => x.ClusterId);
        var clusterIdType = sqlGenerator.ColumnTypeFor<GenericRecordEntry>(x => x.ClusterId, length: 250, nullable: false);

        var agentConnectionIdCol = "agent_connection_id";
        var agentConnectionIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);

        var fingerprintCol = "fingerprint";
        var fingerprintType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);

        var lastUpdatedAtCol = "last_updated_at";
        var lastUpdatedAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);

        var columns = new List<string>
        {
            $"{clusterIdCol} {clusterIdType}",
            $"{agentConnectionIdCol} {agentConnectionIdType}",
            $"{fingerprintCol} {fingerprintType}",
            $"{lastUpdatedAtCol} {lastUpdatedAtType}"
        };

        var pkName = sqlGenerator.NormalizeIdentifierForDb($"pk_{tableName}agent_conn_fingerprint");
        var pk = $" CONSTRAINT {pkName} PRIMARY KEY ({clusterIdCol}, {agentConnectionIdCol})";

        var create = $"CREATE TABLE {tableName} ({string.Join(", \n ", columns)}, \n {pk});";

        return create;
    }
}