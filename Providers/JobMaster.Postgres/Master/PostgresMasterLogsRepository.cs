using JobMaster.Sdk.Abstractions.Config;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;

namespace JobMaster.Postgres.Master;

internal sealed class PostgresMasterLogsRepository : SqlMasterLogsRepository
{
    public PostgresMasterLogsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connManager) : base(clusterConnectionConfig, connManager)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;
}
