using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.SqlBase.Agents;
using JobMaster.SqlBase.Connections;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.SqlServer.Agents;

internal class SqlServerAgentRawFingerprintResolver : SqlAgentRawFingerprintResolver
{
    public SqlServerAgentRawFingerprintResolver(IServiceProvider serviceProvider) :
        base(serviceProvider.GetRequiredKeyedService<IDbConnectionManager>(SqlServerRepositoryConstants.RepositoryTypeId),
            serviceProvider.GetRequiredService<IKnownExceptionIdentifier>())
    {
    }

    public override string AgentRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
}
