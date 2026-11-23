using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;

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
        var token = JobMasterRandomUtil.NewGuid4().ToString("N");

        using var conn = connManager.Open(connString, additionalConnConfig);

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
