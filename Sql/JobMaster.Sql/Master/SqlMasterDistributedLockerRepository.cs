using System.Collections.Concurrent;
using System.Data;
using Dapper;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Keys;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Repositories.Master;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sql.Connections;
using JobMaster.Sql.Scripts;

namespace JobMaster.Sql.Master;

public abstract class SqlMasterDistributedLockerRepository : JobMasterClusterAwareRepository, IMasterDistributedLockerRepository, IDisposable
{
    protected IDbConnectionManager connManager = null!;
    private ISqlGenerator sql = null!;
    protected string connString = string.Empty;
    protected JobMasterConfigDictionary additionalConnConfig = null!;
    protected string connectionId = string.Empty;
    protected IJobMasterLogger logger = null!;

    private readonly Timer CleanupTimers;
    private JobMasterLockKeys lockKeys = null!;

    public SqlMasterDistributedLockerRepository(JobMasterClusterConnectionConfig clusterConnectionConfig, IDbConnectionManager connManager, IJobMasterLogger logger) : base(clusterConnectionConfig)
    {
        this.connManager = connManager;
        sql = SqlGeneratorFactory.Get(this.MasterRepoTypeId);
        connString = clusterConnectionConfig.ConnectionString;
        additionalConnConfig = clusterConnectionConfig.AdditionalConnConfig;
        this.logger = logger;
        connectionId = $"{nameof(SqlMasterDistributedLockerRepository)}:{clusterConnectionConfig.ClusterId}:{Guid.NewGuid().ToString("N")}";
        
        CleanupTimers = new Timer(_ => SafeCleanupExpiredLocks(), null, TimeSpan.FromHours(2), TimeSpan.FromHours(2));
        lockKeys = new JobMasterLockKeys(clusterConnectionConfig.ClusterId);
    }

    private void SafeCleanupExpiredLocks()
    {
        string? lockToken = null;
        try
        {
            lockToken = this.TryLock(lockKeys.LockCleanupLock(), TimeSpan.FromHours(2)); // only release in case of failure. 
            if (lockToken == null)
            {
                return;
            }
            
            var cutoff = DateTime.UtcNow.Subtract(TimeSpan.FromDays(2));
            var t = TableName();
            var sqlText = $"DELETE FROM {t} WHERE {ColClusterId()} = @ClusterId AND {ColExpiresAt()} < @Cutoff";

            using var conn = connManager.Open(connString, additionalConnConfig);
            var deletedCount = conn.Execute(sqlText, new { ClusterId = ClusterConnConfig.ClusterId, Cutoff = cutoff });
            
            if (deletedCount > 0)
            {
                logger.Warn($"Zombie Locks Detected: Cleanup removed {deletedCount} locks that were expired more than 48h. Investigate worker stability.", 
                    JobMasterLogSubjectType.Cluster, ClusterConnConfig.ClusterId);
            }
        }
        catch (Exception e)
        {
            logger.Error($"Failed to cleanup expired locks", JobMasterLogSubjectType.Cluster, ClusterConnConfig.ClusterId, exception: e);
            if (!string.IsNullOrEmpty(lockToken))
            {
                this.ReleaseLock(lockKeys.LockCleanupLock(), lockToken!);
            }
        }
    }

    protected string TableName()
    {
        var tablePrefix = sql.GetTablePrefix(additionalConnConfig);
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
        // Keep fixed table name for values to avoid model coupling
        return $"{prefix}distributed_lock";
    }

    protected string ColClusterId() => "cluster_id";
    protected string ColKey() => "lock_key";
    protected string ColExpiresAt() => "expires_at";
    protected string ColLockToken() => "lock_token";

    public virtual string? TryLock(string key, TimeSpan leaseDuration)
    {
        using var keepAliveCnn = connManager.AcquireConnection(connectionId, TimeSpan.FromMinutes(10), connString, additionalConnConfig, maxGates: 1);
        var conn = keepAliveCnn.Connection;
        if (conn == null)
        {
           throw new Exception("Failed to acquire connection.");
        }

        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);
        try
        {
            var now = DateTime.UtcNow;
            var newExpires = now.Add(leaseDuration);
            var t = TableName();
            var token = Guid.NewGuid().ToString("N");

            // 1) Try to update an expired lock row
            var updateSql = $@"UPDATE {t}
SET {ColExpiresAt()} = @NewExpires,
    {ColLockToken()} = @Token
WHERE {ColClusterId()} = @ClusterId AND {ColKey()} = @Key AND ({ColExpiresAt()} IS NULL OR {ColExpiresAt()} < @Now)";
            var affected = conn.Execute(updateSql, new { ClusterId = ClusterConnConfig.ClusterId, Key = key, Now = now, NewExpires = newExpires, Token = token }, tx);
            if (affected > 0)
            {
                tx.Commit();
                return token;
            }

            // 2) Try to insert a new lock row
            var insertSql = $@"INSERT INTO {t} ({ColClusterId()}, {ColKey()}, {ColExpiresAt()}, {ColLockToken()}) VALUES (@ClusterId, @Key, @NewExpires, @Token)";
            try
            {
                conn.Execute(insertSql, new { ClusterId = ClusterConnConfig.ClusterId, Key = key, NewExpires = newExpires, Token = token }, tx);
                tx.Commit();
                return token;
            }
            catch
            {
                // Likely unique constraint violation (already locked and not expired)
                tx.Rollback();
                return null;
            }
        }
        catch
        {
            try { tx?.Rollback(); } catch { /* ignore */ }
            throw;
        }
    }

    public virtual bool ReleaseLock(string key, string lockToken)
    {
        using var keepAliveCnn = connManager.AcquireConnection(connectionId, TimeSpan.FromMinutes(10), connString, additionalConnConfig, maxGates: 1);
        var conn = keepAliveCnn.Connection;
        if (conn == null)
        {
            // TODO: Log error.
            return false;
        }

        var t = TableName();
        
        var affected = conn.Execute(
            $"DELETE FROM {t} WHERE {ColClusterId()} = @ClusterId AND {ColKey()} = @Key AND {ColLockToken()} = @Token",
            new { ClusterId = ClusterConnConfig.ClusterId, Key = key, Token = lockToken });
        return affected > 0;
    }

    public virtual bool IsLocked(string key)
    {
        using var keepAliveCnn = connManager.AcquireConnection(connectionId, TimeSpan.FromMinutes(10), connString, additionalConnConfig, maxGates: 1);
        var conn = keepAliveCnn.Connection;

        if (conn is null)
        {
            throw  new Exception("Failed to acquire connection.");
        }

        var t = TableName();
        var now = DateTime.UtcNow;
        var sqlText = $"SELECT 1 FROM {t} WHERE {ColClusterId()} = @ClusterId AND {ColKey()} = @Key AND {ColExpiresAt()} > @Now";
        var found = conn.ExecuteScalar<int?>(sqlText, new { ClusterId = ClusterConnConfig.ClusterId, Key = key, Now = now });
        return found.HasValue;
    }

    public bool ForceReleaseLock(string key)
    {
        using var keepAliveCnn = connManager.AcquireConnection(connectionId, TimeSpan.FromMinutes(10), connString, additionalConnConfig, maxGates: 1);
        var conn = keepAliveCnn.Connection;
        if (conn == null)
        {
            // TODO: Log error.
            return false;
        }

        var t = TableName();
        
        var affected = conn.Execute(
            $"DELETE FROM {t} WHERE {ColClusterId()} = @ClusterId AND {ColKey()} = @Key",
            new { ClusterId = ClusterConnConfig.ClusterId, Key = key });
        return affected > 0;
    }

    public void Dispose()
    {
        CleanupTimers?.Dispose();
    }
}