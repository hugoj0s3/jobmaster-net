using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.SqlBase.Agents;
using JobMaster.SqlBase.Connections;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.MySql.Agents;

internal class MySqlAgentFootprintResolver : SqlAgentFootprintResolver
{
    public MySqlAgentFootprintResolver(IServiceProvider serviceProvider) : 
        base(serviceProvider.GetRequiredKeyedService<IDbConnectionManager>(MySqlRepositoryConstants.RepositoryTypeId))
    {
    }

    public override string AgentRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;
}
