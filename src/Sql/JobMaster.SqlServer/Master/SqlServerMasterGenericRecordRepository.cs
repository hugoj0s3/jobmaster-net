using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sql.Connections;
using JobMaster.Sql.Master;

namespace JobMaster.SqlServer.Master;

internal class SqlServerMasterGenericRecordRepository : SqlMasterGenericRecordRepository
{
    public SqlServerMasterGenericRecordRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
}
