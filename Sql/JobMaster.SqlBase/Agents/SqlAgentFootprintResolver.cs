using System;
using System.Threading.Tasks;
using Dapper;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Scripts;

namespace JobMaster.SqlBase.Agents;

internal abstract class SqlAgentFootprintResolver : IAgentFootprintResolver
{
    private readonly IDbConnectionManager dbConnectionManager;

    private JobMasterConfigDictionary additionalConnConfig;
    private string connString;
    private ISqlGenerator sql;

    public SqlAgentFootprintResolver(IDbConnectionManager dbConnectionManager)
    {
        this.dbConnectionManager = dbConnectionManager;
    }
    
    public async ValueTask<string> GiveYourFootprintAsync(string clusterId, string agentConnectionId)
    {
        using var connection = await dbConnectionManager.OpenAsync(connString, additionalConnConfig, ReadIsolationLevel.Consistent);
        var footprint = await connection.QueryFirstOrDefaultAsync<string>(@$"
SELECT footprint 
FROM {FootprintTableName()} 
where cluster_id = @clusterId and 
      agent_connection_id = @agentConnectionId", new { clusterId, agentConnectionId });
        
        if (!string.IsNullOrEmpty(footprint))
        {
            return footprint;
        }

        footprint = Guid.NewGuid().ToString();
        
        // insert footprint 
        await connection.ExecuteAsync($@"
INSERT INTO {FootprintTableName()} (cluster_id, agent_connection_id, footprint, last_updated_at)
SELECT @clusterId, @agentConnectionId, @footprint, @lastUpdatedAt
WHERE NOT EXISTS (
    SELECT 1
    FROM {FootprintTableName()}
    WHERE cluster_id = @clusterId
      AND agent_connection_id = @agentConnectionId
);

UPDATE {FootprintTableName()}
SET footprint = @footprint,
    last_update_at = @lastUpdatedAt
WHERE cluster_id = @clusterId
  AND agent_connection_id = @agentConnectionId;
",
            new { clusterId, agentConnectionId, footprint, lastUpdatedAt = DateTime.UtcNow });

        return footprint;
    }
    
    public void Initialize(JobMasterAgentConnectionConfig config)
    {
        this.connString = config.ConnectionString;    
        this.additionalConnConfig = config.AdditionalConnConfig;
        this.sql = SqlGeneratorFactory.Get(this.AgentRepoTypeId);
    }

    public abstract string AgentRepoTypeId { get; }

    protected string FootprintTableName()
    {
        var tablePrefix = sql.GetTablePrefix(additionalConnConfig);
        var prefix = string.IsNullOrEmpty(tablePrefix) ? string.Empty : tablePrefix;
        return $"{prefix}footprint";
    }
}