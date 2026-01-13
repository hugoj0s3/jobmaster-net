using JobMaster.Sql.Connections;
using JobMaster.Sql.Master;
using JobMaster.Sdk.Contracts.Config;

namespace JobMaster.SqlServer.Master;

internal class SqlServerMasterRecurringSchedulesRepository : SqlMasterRecurringSchedulesRepository
{
    public SqlServerMasterRecurringSchedulesRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
}
