using JobMaster.Sdk.Abstractions.Config;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;

namespace JobMaster.SqlServer.Master;

internal sealed class SqlServerMasterLogsRepository : SqlMasterLogsRepository
{
    public SqlServerMasterLogsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connManager) : base(clusterConnectionConfig, connManager)
    {
    }

    public override string MasterRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
}
