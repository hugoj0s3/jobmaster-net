using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.SqlBase.Agents;
using JobMaster.SqlBase.Connections;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.MySql.Agents;

internal class MySqlAgentFingerprintResolver : SqlAgentFingerprintResolver
{
    public MySqlAgentFingerprintResolver(IServiceProvider serviceProvider) :
        base(serviceProvider.GetRequiredKeyedService<IDbConnectionManager>(MySqlRepositoryConstants.RepositoryTypeId),
            serviceProvider.GetRequiredService<IKnownExceptionIdentifier>())
    {
    }

    public override string AgentRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;
}
