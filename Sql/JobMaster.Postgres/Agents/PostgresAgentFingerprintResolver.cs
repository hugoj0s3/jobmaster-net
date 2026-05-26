using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.SqlBase.Agents;
using JobMaster.SqlBase.Connections;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.Postgres.Agents;

internal class PostgresAgentFingerprintResolver : SqlAgentFingerprintResolver
{
    public PostgresAgentFingerprintResolver(IServiceProvider serviceProvider) : 
        base(serviceProvider.GetRequiredKeyedService<IDbConnectionManager>(PostgresRepositoryConstants.RepositoryTypeId))
    {
    }

    public override string AgentRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;
    
    
    
    
    
    /// Updated.
}