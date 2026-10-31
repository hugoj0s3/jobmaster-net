using System.Data;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Utils;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlBase.Agents;

internal abstract class SqlAgentFingerprintResolver : IAgentFingerprintResolver
{
    private readonly IDbConnectionManager dbConnectionManager;
    private readonly IKnownExceptionIdentifier knownExceptionIdentifier;

    private JobMasterConfigDictionary additionalConnConfig = null!;
    private string connString = string.Empty;
    private ISqlGenerator sql = null!;

    protected SqlAgentFingerprintResolver(IDbConnectionManager dbConnectionManager, IKnownExceptionIdentifier knownExceptionIdentifier)
    {
        this.dbConnectionManager = dbConnectionManager;
        this.knownExceptionIdentifier = knownExceptionIdentifier;
    }

    public async ValueTask<string> GiveYourFingerprintAsync(string clusterId, string agentConnectionId)
    {
        using var connection = await dbConnectionManager.OpenAsync(connString, additionalConnConfig);

        var fingerprint = await ReadFingerprintAsync(connection, clusterId, agentConnectionId);
        if (!string.IsNullOrEmpty(fingerprint))
        {
            return fingerprint!;
        }

        fingerprint = JobMasterRandomUtil.NewGuid4().ToString();

        try
        {
            // Plain insert relying on the (cluster_id, agent_connection_id) primary key for
            // atomicity — a concurrent insert from another process racing to create the same
            // row for the first time fails with a duplicate-key error instead of both silently
            // succeeding and one overwriting the other's value.
            await connection.ExecuteAsync($@"
INSERT INTO {FingerprintTableName()} (cluster_id, agent_connection_id, fingerprint, last_updated_at)
VALUES (@clusterId, @agentConnectionId, @fingerprint, @lastUpdatedAt);",
                new { clusterId, agentConnectionId, fingerprint, lastUpdatedAt = DateTime.UtcNow });

            return fingerprint;
        }
        catch (Exception ex) when (knownExceptionIdentifier.Identify(AgentRepoTypeId, ex) == JobMasterKnownExceptionId.DuplicateKey)
        {
            // Someone else won the race and inserted first — defer to whatever they persisted
            // instead of returning our own locally-generated value, which would silently
            // diverge from what's actually stored.
            var persisted = await ReadFingerprintAsync(connection, clusterId, agentConnectionId);
            return persisted!;
        }
    }

    private async Task<string?> ReadFingerprintAsync(IDbConnection connection, string clusterId, string agentConnectionId)
    {
        return await connection.QueryFirstOrDefaultAsync<string>(@$"
SELECT fingerprint
FROM {FingerprintTableName()}
where cluster_id = @clusterId and
      agent_connection_id = @agentConnectionId", new { clusterId, agentConnectionId });
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
