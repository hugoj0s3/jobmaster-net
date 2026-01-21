using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sql.Connections;
using JobMaster.Sql.Master;

namespace JobMaster.SqlServer.Master;

internal class SqlServerMasterDistributedLockerRepository : SqlMasterDistributedLockerRepository
{
    public SqlServerMasterDistributedLockerRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connManager,
        IJobMasterLogger logger) : base(clusterConnectionConfig, connManager, logger)
    {
    }

    public override string MasterRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;

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
        var sqlText = $@"
MERGE {t} WITH (HOLDLOCK) AS target
USING (SELECT @ClusterId AS {ColClusterId()}, @Key AS {ColKey()}) AS src
ON target.{ColClusterId()} = src.{ColClusterId()} AND target.{ColKey()} = src.{ColKey()}
WHEN MATCHED AND (target.{ColExpiresAt()} IS NULL OR target.{ColExpiresAt()} < @Now)
    THEN UPDATE SET {ColExpiresAt()} = @NewExpires, {ColLockToken()} = @Token
WHEN NOT MATCHED
    THEN INSERT ({ColClusterId()}, {ColKey()}, {ColExpiresAt()}, {ColLockToken()})
         VALUES (@ClusterId, @Key, @NewExpires, @Token)
OUTPUT inserted.{ColLockToken()};";

        var acquiredToken = conn.ExecuteScalar<string?>(sqlText, new
        {
            ClusterId = ClusterConnConfig.ClusterId,
            Key = key,
            Now = now,
            NewExpires = newExpires,
            Token = token
        });

        return acquiredToken == token ? token : null;
    }
}
