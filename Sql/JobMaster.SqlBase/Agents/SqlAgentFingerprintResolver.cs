using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Utils;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlBase.Agents;

internal abstract class SqlAgentFingerprintResolver : IAgentFingerprintResolver
{
    private readonly IDbConnectionManager dbConnectionManager;

    private JobMasterConfigDictionary additionalConnConfig = null!;
    private string connString = string.Empty;
    private ISqlGenerator sql = null!;

    protected SqlAgentFingerprintResolver(IDbConnectionManager dbConnectionManager)
    {
        this.dbConnectionManager = dbConnectionManager;
    }

    public async ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId)
    {
        using var connection = await dbConnectionManager.OpenAsync(connString, additionalConnConfig);
        var fingerprint = await connection.QueryFirstOrDefaultAsync<string>(@$"
SELECT fingerprint
FROM {FingerprintTableName()}
where cluster_id = @clusterId and
      agent_connection_id = @agentConnectionId", new { clusterId, agentConnectionId });

        if (!string.IsNullOrEmpty(fingerprint))
        {
            return fingerprint!;
        }

        fingerprint = JobMasterRandomUtil.NewGuid4().ToString();

        // insert fingerprint
        await connection.ExecuteAsync($@"
INSERT INTO {FingerprintTableName()} (cluster_id, agent_connection_id, fingerprint, last_updated_at)
SELECT @clusterId, @agentConnectionId, @fingerprint, @lastUpdatedAt
WHERE NOT EXISTS (
    SELECT 1
    FROM {FingerprintTableName()}
    WHERE cluster_id = @clusterId
      AND agent_connection_id = @agentConnectionId
);

UPDATE {FingerprintTableName()}
SET fingerprint = @fingerprint,
    last_updated_at = @lastUpdatedAt
WHERE cluster_id = @clusterId
  AND agent_connection_id = @agentConnectionId;
",
            new { clusterId, agentConnectionId, fingerprint, lastUpdatedAt = DateTime.UtcNow });

        return fingerprint;
    }

    public void Initialize(JobMasterAgentConnectionConfig config)
    {
        this.connString = config.ConnectionString;
        this.additionalConnConfig = config.AdditionalConnConfig;
        this.sql = SqlGeneratorFactory.Get(this.AgentRepoTypeId);
    }

    public abstract string AgentRepoTypeId { get; }

    private string FingerprintTableName()
    {
        return $"{sql.GetTablePrefix(additionalConnConfig)}agent_conn_fingerprint";
    }
}
