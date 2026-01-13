using System.Collections.Concurrent;
using System.Data;
using System.Text;
using Dapper;
using JobMaster.Contracts.Extensions;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Connections;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Repositories.Agent;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sql.Connections;
using JobMaster.Sql.Scripts;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Services.Master;

namespace JobMaster.Sql.Agents;

public abstract class SqlRawMessagesDispatcherRepositoryBase : JobMasterClusterAwareComponent, IAgentRawMessagesDispatcherRepository
{
    private static readonly TimeSpan ConnectionIdleTimeout = TimeSpan.FromMinutes(10);
    
    protected IDbConnectionManager connManager;
    protected readonly IJobMasterLogger logger;
    protected JobMasterConfigDictionary additionalConnConfig = null!;
    protected string connString = null!;
    protected ISqlGenerator sql = null!;
    private string connectionIdPrefix = null!;


    protected SqlRawMessagesDispatcherRepositoryBase(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IDbConnectionManager connManager,
        IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.connManager = connManager;
        this.logger = logger;
    }

    public void Initialize(JobMasterAgentConnectionConfig config)
    {
        this.connString = config.ConnectionString;    
        this.additionalConnConfig = config.AdditionalConnConfig;
        this.sql = SqlGeneratorFactory.Get(this.AgentRepoTypeId);
        this.connectionIdPrefix = nameof(SqlRawMessagesDispatcherRepositoryBase) + ":" + config.Id;
    }

    protected string MessageTableName()
    {
        var tablePrefix = sql.GetTablePrefix(additionalConnConfig);
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
        return $"{prefix}message_dispatcher";
    }

    protected string BucketTableName()
    {
        var tablePrefix = sql.GetTablePrefix(additionalConnConfig);
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
        return $"{prefix}bucket_dispatcher";
    }

    public virtual bool IsAutoDequeue => false;
    public abstract string AgentRepoTypeId { get; }

    protected virtual void PushMessageCore(IDbConnection cnn, IDbTransaction transaction, string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId)
    {
        var sqlText = GetInsertSql();
        var messageId = GenerateMessageId();
        cnn.Execute(sqlText, new
        {
            BucketId = fullBucketAddressId,
            MessageId = messageId,
            Payload = payload,
            RefTime = referenceTime.ToUniversalTime(),
            CorrelationId = correlationId,
            EnqueuedAt = DateTime.UtcNow,
        }, transaction);
    }
    
    public virtual string PushMessage(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId)
    {
        // using var cnnKeepAlive = AcquireConnectionKeepAlive();
        // var cnn = cnnKeepAlive.Connection;
        // if (cnn == null)
        // {
        //     throw new Exception("Failed to acquire connection.");
        // }
        
        using var cnn = connManager.Open(connString, additionalConnConfig);
        
        
        using var transaction = cnn.BeginTransaction(IsolationLevel.ReadCommitted);

        try
        {
            var sqlText = GetInsertSql();
            var messageId = GenerateMessageId();
            cnn.Execute(sqlText, new
            {
                BucketId = fullBucketAddressId,
                MessageId = messageId,
                Payload = payload,
                RefTime = referenceTime.ToUniversalTime(),
                CorrelationId = correlationId,
                EnqueuedAt = DateTime.UtcNow,
            }, transaction);
            transaction.Commit();
            return messageId;
        }
        catch (Exception)
        {
            transaction.Rollback();
            throw;
        }
    }

    protected virtual (string Table, string ColMsgId, string ColBucket, string SelectSql) GetDequeueSelectSql(
        int numberOfJobs,
        bool referenceTimeToHasValue)
    {
        var table = MessageTableName();
        var colMsgId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.MessageId);
        var colPayload = sql.ColumnNameFor<JobMasterRawMessage>(x => x.Payload);
        var colRefTime = sql.ColumnNameFor<JobMasterRawMessage>(x => x.ReferenceTime);
        var colCorrId = sql.ColumnNameFor<JobMasterRawMessage>(x => x.CorrelationId);
        var colEnqAt = sql.ColumnNameFor<JobMasterRawMessage>(x => x.EnqueuedAt);
        var colBucket = "bucket_address_id";

        var selectSql = $@"SELECT {colMsgId}, {colPayload}, {colRefTime}, {colCorrId}, {colEnqAt}
FROM {table}
WHERE {colBucket} = @Bucket";

        if (referenceTimeToHasValue)
        {
            selectSql += $" AND {colRefTime} <= @RefTo";
        }

        selectSql += $" ORDER BY {colRefTime} ASC, {colMsgId} ASC";
        selectSql += sql.OffsetQueryFor(numberOfJobs);

        return (table, colMsgId, colBucket, selectSql);
    }
    
    public virtual async Task<IList<string>> BulkPushMessageAsync(string fullBucketAddressId, IList<(string payload, DateTime referenceTime, string correlationId)> messages)
    {
        using var cnn = await this.connManager.OpenAsync(this.connString, additionalConnConfig);
        
        using var transaction = cnn.BeginTransaction(IsolationLevel.ReadCommitted);

        try
        {
            var ids = new List<string>(messages.Count);
            var partitions = messages.Partition(JobMasterConstants.MaxBatchSizeForBulkOperation);
            foreach (var partition in partitions)
            {
                var parameters = new Dictionary<string, object>
                {
                    { "BucketId", fullBucketAddressId }
                };
                var sqlTextBuilder = new StringBuilder();
                sqlTextBuilder.AppendLine(GetInsertHeaderSql());
                sqlTextBuilder.AppendLine("VALUES");

                for (int i = 0; i < partition.Count; i++)
                {
                    var (payload, referenceTime, correlationId) = partition.ElementAt(i);
                    var msgId = GenerateMessageId();
                    ids.Add(msgId);

                    var payloadParameter = $"Payload_{i}";
                    var refTimeParameter = $"RefTime_{i}";
                    var corrIdParameter = $"CorrelationId_{i}";
                    var enqAtParameter = $"EnqAt_{i}";
                    var msgIdParameter = $"MessageId_{i}";

                    parameters.Add(msgIdParameter, msgId);
                    parameters.Add(payloadParameter, payload);
                    parameters.Add(refTimeParameter, referenceTime.ToUniversalTime());
                    parameters.Add(corrIdParameter, correlationId);
                    parameters.Add(enqAtParameter, DateTime.UtcNow);

                    var valuesClause = $"@BucketId, @{msgIdParameter}, @{payloadParameter}, @{refTimeParameter}, @{corrIdParameter}, @{enqAtParameter}";
                    sqlTextBuilder.Append("(").Append(valuesClause).Append(")");
                    if (i < partition.Count - 1)
                    {
                        sqlTextBuilder.AppendLine(",");
                    }
                    else
                    {
                        sqlTextBuilder.AppendLine();
                    }
                }

                var bulkSqlText = sqlTextBuilder.ToString();
                await cnn.ExecuteAsync(bulkSqlText, parameters, transaction);
            }

            transaction.Commit();
            return ids;
        }
        catch (Exception)
        {
            transaction.Rollback();
            throw;
        }
    }
    
    public virtual async Task<string> PushMessageAsync(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId)
    {
        // using var cnnKeepAlive = AcquireConnectionKeepAlive();
        // var cnn = cnnKeepAlive.Connection;
        // if (cnn == null)
        // {
        //     throw new Exception("Failed to acquire connection.");
        // }
        using var cnn = await connManager.OpenAsync(connString, additionalConnConfig);
        
        using var transaction = cnn.BeginTransaction(IsolationLevel.ReadCommitted);
        
        try
        {
            var sqlText = GetInsertSql();
            var messageId = GenerateMessageId();
            await cnn.ExecuteAsync(sqlText, new
            {
                BucketId = fullBucketAddressId,
                MessageId = messageId,
                Payload = payload,
                RefTime = referenceTime.ToUniversalTime(),
                CorrelationId = correlationId,
                EnqueuedAt = DateTime.UtcNow
            }, transaction);
            transaction.Commit();
            return messageId;
        }
        catch (Exception)
        {
            transaction.Rollback();
            throw;
        }
    }

   
    
    public virtual async Task<IList<JobMasterRawMessage>> DequeueMessagesAsync(string fullBucketAddressId, int numberOfJobs, DateTime? referenceTimeTo = null)
    {
        using var cnnKeepAlive = AcquireConnectionKeepAlive();
        var cnn = cnnKeepAlive.Connection;
        if (cnn == null)
        { 
           logger.Error($"Failed to acquire keep-alive connection for DequeueMessagesAsync.");
           return new List<JobMasterRawMessage>();
        }
        
        // using var cnn = await this.connManager.OpenAsync(this.connString, additionalConnConfig);
        using var tx = cnn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var result = await DequeueMessagesAsyncCore(cnn, tx, fullBucketAddressId, numberOfJobs, referenceTimeTo);
            tx.Commit();
            return result;
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }
    
    protected virtual async Task<IList<JobMasterRawMessage>> DequeueMessagesAsyncCore(IDbConnection cnn, IDbTransaction tx, string fullBucketAddressId, int numberOfJobs, DateTime? referenceTimeTo = null)
    {
        var q = GetDequeueSelectSql(numberOfJobs, referenceTimeTo.HasValue);

        var rows = await cnn.QueryAsync<JobMasterRawMessagePersistenceRecord>(q.SelectSql, new
        {
            Bucket = fullBucketAddressId,
            RefTo = (referenceTimeTo ?? DateTime.UtcNow).ToUniversalTime()
        }, tx);

        var picked = rows.Take(numberOfJobs).ToList();
        var ids = picked.Select(r => r.MessageId).ToList();

        if (ids.Count > 0)
        {
            var deleteSql = $"DELETE FROM {q.Table} WHERE {q.ColBucket} = @Bucket AND {this.sql.InClauseFor(q.ColMsgId, "@Ids")}";
            await cnn.ExecuteAsync(deleteSql, new { Bucket = fullBucketAddressId, Ids = ids }, tx);
        }

        return picked.Select(r => JobMasterRawMessage.RecoverFromDb(r)).ToList();
    }

    protected virtual bool HasJobsCore(IDbConnection cnn, string fullBucketAddressId)
    {
        var table = MessageTableName();
        var colBucket = "bucket_address_id";
        var count = cnn.ExecuteScalar<long>($"SELECT COUNT(1) FROM {table} WHERE {colBucket} = @Bucket",
            new { Bucket = fullBucketAddressId });
        return count > 0;
    }
    
    private bool HasJobs(string fullBucketAddressId) // TODO remove after async version is implemented.
    {
        using var cnn = this.connManager.Open(this.connString, additionalConnConfig);
        return HasJobsCore(cnn, fullBucketAddressId);
    }

    public Task<bool> HasJobsAsync(string fullBucketAddressId) // TODO remove after async version is implemented.
    {
        // TODO implement real async version.
        var result = HasJobs(fullBucketAddressId);
        return Task.FromResult(result);
    }

    protected virtual void CreateBucketCore(IDbConnection cnn, IDbTransaction transaction, string fullBucketAddressId)
    {
        var bucketQueueTable = BucketTableName();
        var colBucket = "bucket_address_id";
        var insertSql = $"INSERT INTO {bucketQueueTable} ({colBucket}) VALUES (@Bucket)";
        cnn.Execute(insertSql, new { Bucket = fullBucketAddressId }, transaction);
    }
    
    public Task CreateBucketAsync(string fullBucketAddressId) // TODO remove after async version is implemented.
    {
        // TODO implement real async version.
        CreateBucket(fullBucketAddressId);
        return Task.CompletedTask;
    }
    
    public Task DestroyBucketAsync(string fullBucketAddressId) // TODO remove after async version is implemented.
    {
        // TODO implement real async version.
        DestroyBucket(fullBucketAddressId);
        return Task.CompletedTask;
    }
    
    private void CreateBucket(string fullBucketAddressId)
    {
        using var cnn = this.connManager.Open(this.connString, additionalConnConfig);
        using var transaction = cnn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            CreateBucketCore(cnn, transaction, fullBucketAddressId);
            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    protected virtual void DestroyBucketCore(IDbConnection cnn, IDbTransaction transaction, string fullBucketAddressId)
    {
        var table = MessageTableName();
        var colBucket = "bucket_address_id";
        var deleteSql = $"DELETE FROM {table} WHERE {colBucket} = @Bucket";

        cnn.Execute(deleteSql, new { Bucket = fullBucketAddressId }, transaction);
        cnn.Execute($"DELETE FROM {BucketTableName()} WHERE bucket_address_id = @Bucket", new { Bucket = fullBucketAddressId }, transaction);
    }
    
    public virtual void DestroyBucket(string fullBucketAddressId)
    {
        using var cnn = this.connManager.Open(this.connString, additionalConnConfig);
        using var transaction = cnn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            DestroyBucketCore(cnn, transaction, fullBucketAddressId);
            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    private const int MaxOfSlots = 7;
    private const int MaxOfGatesPerSlot = 3;
    private int currentSlotIndex = -1;
    private IAcquirableKeepAliveConnection<IDbConnection> AcquireConnectionKeepAlive()
    {
        var nextVal = Interlocked.Increment(ref currentSlotIndex);
        
        // Math.Abs prevent overflow.
        var slotIndex = Math.Abs(nextVal % MaxOfSlots);
        
        var slotName = $"{connectionIdPrefix}{slotIndex}";
        
        var cnnKeepAlive = 
            this.connManager.AcquireConnection($"{slotName}", 
                ConnectionIdleTimeout, 
                this.connString, 
                additionalConnConfig, 
                maxGates: MaxOfGatesPerSlot);

        return cnnKeepAlive;
    }

    // Helpers
    protected virtual string GetInsertSql(string? payloadParameter = null, string? refTimeParameter = null, string? corrIdParameter = null, string? enqAtParameter = null)
    {
        payloadParameter ??= "@Payload";
        refTimeParameter ??= "@RefTime";
        corrIdParameter ??= "@CorrelationId";
        enqAtParameter ??= "@EnqueuedAt";

        if (!payloadParameter.StartsWith("@"))
        {
            payloadParameter = $"@{payloadParameter}";
        }
        
        if (!refTimeParameter.StartsWith("@"))
        {
            refTimeParameter = $"@{refTimeParameter}";
        }
        
        if (!corrIdParameter.StartsWith("@"))
        {
            corrIdParameter = $"@{corrIdParameter}";
        }
        
        if (!enqAtParameter.StartsWith("@"))
        {
            enqAtParameter = $"@{enqAtParameter}";
        }
        
        return $"{GetInsertHeaderSql()} VALUES ({GetInsertValuesClause(payloadParameter, refTimeParameter, corrIdParameter, enqAtParameter)})"; 
    }

    protected virtual string GenerateMessageId() => Guid.NewGuid().ToString("D");

    // New helpers to compose INSERTs and reuse in bulk
    protected virtual string GetInsertHeaderSql()
    {
        return @$"INSERT INTO {MessageTableName()} (
bucket_address_id,
{sql.ColumnNameFor<JobMasterRawMessage>(x => x.MessageId)},
{sql.ColumnNameFor<JobMasterRawMessage>(x => x.Payload)},
{sql.ColumnNameFor<JobMasterRawMessage>(x => x.ReferenceTime)},
{sql.ColumnNameFor<JobMasterRawMessage>(x => x.CorrelationId)},
{sql.ColumnNameFor<JobMasterRawMessage>(x => x.EnqueuedAt)})";
    }

    protected virtual string GetInsertValuesClause(string payloadParameter = "@Payload", string refTimeParameter = "@RefTime", string corrIdParameter = "@CorrelationId", string enqAtParameter = "@EnqueuedAt")
    {
        if (!payloadParameter.StartsWith("@")) payloadParameter = $"@{payloadParameter}";
        if (!refTimeParameter.StartsWith("@")) refTimeParameter = $"@{refTimeParameter}";
        if (!corrIdParameter.StartsWith("@")) corrIdParameter = $"@{corrIdParameter}";
        if (!enqAtParameter.StartsWith("@")) enqAtParameter = $"@{enqAtParameter}";

        return $"@BucketId, @MessageId, {payloadParameter}, {refTimeParameter}, {corrIdParameter}, {enqAtParameter}";
    }
} 