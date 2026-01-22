using JobMaster.Sdk.Abstractions.Config;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;

namespace JobMaster.MySql.Master;

internal class MySqlMasterRecurringSchedulesRepository : SqlMasterRecurringSchedulesRepository
{
    public MySqlMasterRecurringSchedulesRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;
}
