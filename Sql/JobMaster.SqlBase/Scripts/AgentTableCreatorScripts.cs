using JobMaster.Sdk.Abstractions.Models.GenericRecords;

namespace JobMaster.SqlBase.Scripts;

internal class AgentTableCreatorScripts
{
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

        // Types (provider decides exact SQL types)
        // MessageId is now an application-generated GUID string
        var msgIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 64, nullable: false);
        var payloadType = sqlGenerator.ColumnTypeFor(typeof(string), isMaxLength: true, nullable: false);
        var refTimeType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);
        var corrIdType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);
        var enqAtType = sqlGenerator.ColumnTypeFor(typeof(DateTime), nullable: false);
        var bucketType = sqlGenerator.ColumnTypeFor(typeof(string), length: 250, nullable: false);

        var cols = new List<string>
        {
            $"{msgIdCol} {msgIdType}",
            $"{bucketCol} {bucketType}",
            $"{payloadCol} {payloadType}",
            $"{refTimeCol} {refTimeType}",
            $"{corrIdCol} {corrIdType}",
            $"{enqAtCol} {enqAtType}"
        };

        var pkName = sqlGenerator.NormalizeIdentifierForDb($"pk_{tableName}message_dispatcher");
        var fkName = sqlGenerator.NormalizeIdentifierForDb($"fk_{tableName}message_dispatcher_bucket");
        var pk = $" CONSTRAINT {pkName} PRIMARY KEY ({msgIdCol})";
        var fk = $" CONSTRAINT {fkName} FOREIGN KEY ({bucketCol}) REFERENCES {bucketTable}({bucketCol})";

        var create = $"CREATE TABLE {tableName} ({string.Join(", ", cols)},{pk},{fk});";

        // Index aligned with dequeue filter/order
        var idx = sqlGenerator.CreateIndex($"{tableName}", $"idx_{tableName}_bucket_ref", (bucketCol, false, 250), (refTimeCol, false, null));

        return $"{create}\n{idx}";
    }
}