using JobMaster.Sql.Connections;
using JobMaster.Sql.Master;
using JobMaster.Sdk.Contracts.Config;

namespace JobMaster.MySql.Master;

internal class MySqlMasterGenericRecordRepository : SqlMasterGenericRecordRepository
{
    public MySqlMasterGenericRecordRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;
}
