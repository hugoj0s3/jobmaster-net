using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Services.Master;
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
        var token = Guid.NewGuid().ToString("N");
        using var cnn = connManager.Open(connString, additionalConnConfig);
        if (cnn == null)
        {
            throw new Exception("Failed to acquire connection.");
        }
    
        // 1. Forçamos o uso do tempo do banco (UTC_TIMESTAMP(6))
        // 2. Usamos o tempo de expiração como um parâmetro calculado
        var sql = $@"
        INSERT INTO {TableName()} ({ColClusterId()}, {ColKey()}, {ColLockToken()}, {ColExpiresAt()})
        VALUES (@ClusterId, @Key, @Token, DATE_ADD(UTC_TIMESTAMP(6), INTERVAL @Seconds SECOND))
        ON DUPLICATE KEY UPDATE
            {ColLockToken()} = IF({ColExpiresAt()} < UTC_TIMESTAMP(6), VALUES({ColLockToken()}), {ColLockToken()}),
            {ColExpiresAt()} = IF({ColExpiresAt()} < UTC_TIMESTAMP(6), VALUES({ColExpiresAt()}), {ColExpiresAt()});
    ";

        var rowsAffected = cnn.Execute(sql, new
        {
            ClusterId = ClusterConnConfig.ClusterId,
            Key = key,
            Token = token,
            Seconds = duration.TotalSeconds
        });

        // Se UseAffectedRows=true estiver na connection string:
        // 1 = Novo lock (Sucesso)
        // 2 = Lock antigo expirou e foi substituído (Sucesso)
        // 0 = Lock ainda é válido (Falha - retorna null)
        if (rowsAffected > 0) return token;

        return null;
    }
}