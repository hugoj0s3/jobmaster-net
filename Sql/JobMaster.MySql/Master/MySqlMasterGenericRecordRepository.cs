using JobMaster.Sdk.Abstractions.Config;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;

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
