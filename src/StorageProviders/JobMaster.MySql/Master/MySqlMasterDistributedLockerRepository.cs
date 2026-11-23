using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;

namespace JobMaster.MySql.Master;

internal class MySqlMasterDistributedLockerRepository : SqlMasterDistributedLockerRepository
{
    public MySqlMasterDistributedLockerRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connManager,
        IJobMasterLogger logger) 
        : base(clusterConnectionConfig, connManager, logger)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;

    public override string? TryLock(string key, TimeSpan duration)
    {
        var token = JobMasterRandomUtil.NewGuid4().ToString("N");
        using var cnn = connManager.Open(connString, additionalConnConfig);
        if (cnn == null)
        {
            throw new Exception("Failed to acquire connection.");
        }
    
        var sql = $@"
        INSERT INTO {TableName()} ({ColClusterId()}, {ColKey()}, {ColLockToken()}, {ColExpiresAt()})
        VALUES (@ClusterId, @Key, @Token, DATE_ADD(UTC_TIMESTAMP(6), INTERVAL @Seconds SECOND))
        ON DUPLICATE KEY UPDATE
            {ColLockToken()} = IF({ColExpiresAt()} < UTC_TIMESTAMP(6), VALUES({ColLockToken()}), {ColLockToken()}),
            {ColExpiresAt()} = IF({ColExpiresAt()} < UTC_TIMESTAMP(6), VALUES({ColExpiresAt()}), {ColExpiresAt()});
        SELECT {ColLockToken()} FROM {TableName()} WHERE {ColClusterId()} = @ClusterId AND {ColKey()} = @Key;
    ";

        // MySQL has no RETURNING/OUTPUT clause, unlike the Postgres/SqlServer siblings of this method
        // (which read back the row and compare tokens instead of trusting an affected-rows count) --
        // and relying on rowsAffected here is unsafe: MySqlConnector reports 1, not 0, for the no-op
        // branch above (lock held, not expired, so both SET expressions resolve to the column's own
        // current value), so "rowsAffected > 0" was true even when nothing actually changed, letting
        // TryLock report success while another holder's lock was still active. Reading back the token
        // and comparing -- the same check the other two providers already do -- sidesteps affected-rows
        // semantics entirely: we only "win" if the row's stored token now matches what we tried to write.
        var acquiredToken = cnn.QueryFirstOrDefault<string?>(sql, new
        {
            ClusterId = ClusterConnConfig.ClusterId,
            Key = key,
            Token = token,
            Seconds = duration.TotalSeconds
        });

        return acquiredToken == token ? token : null;
    }
}