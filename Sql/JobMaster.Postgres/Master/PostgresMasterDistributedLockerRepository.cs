using Dapper;
using JobMaster.Sql.Connections;
using JobMaster.Sql.Master;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Services.Master;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterDistributedLockerRepository : SqlMasterDistributedLockerRepository
{
    public PostgresMasterDistributedLockerRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connManager,
        IJobMasterLogger logger) : base(clusterConnectionConfig, connManager, logger)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;

    public override string? TryLock(string key, TimeSpan leaseDuration)
    {
        var now = DateTime.UtcNow;
        var newExpires = now.Add(leaseDuration);
        var token = Guid.NewGuid().ToString("N");

        using var keepAliveCnn = connManager.AcquireConnection(connectionId, TimeSpan.FromMinutes(10), connString, additionalConnConfig, maxGates: 1);
        var conn = keepAliveCnn.Connection;
        if (conn == null)
        {
            throw new Exception("Failed to acquire connection.");
        }

        var t = TableName();
        var sqlText = $@"INSERT INTO {t} ({ColClusterId()}, {ColKey()}, {ColExpiresAt()}, {ColLockToken()})
VALUES (@ClusterId, @Key, @NewExpires, @Token)
ON CONFLICT ({ColClusterId()}, {ColKey()}) DO UPDATE
SET {ColExpiresAt()} = EXCLUDED.{ColExpiresAt()},
    {ColLockToken()} = EXCLUDED.{ColLockToken()}
WHERE {t}.{ColExpiresAt()} IS NULL OR {t}.{ColExpiresAt()} < @Now
RETURNING {ColLockToken()};";

        var acquiredToken = conn.ExecuteScalar<string?>(sqlText, new
        {
            ClusterId = ClusterConnConfig.ClusterId,
            Key = key,
            NewExpires = newExpires,
            Token = token,
            Now = now
        });

        return acquiredToken == token ? token : null;
    }
}
