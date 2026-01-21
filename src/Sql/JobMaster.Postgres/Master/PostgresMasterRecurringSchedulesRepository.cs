using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sql.Connections;
using JobMaster.Sql.Master;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterRecurringSchedulesRepository : SqlMasterRecurringSchedulesRepository
{
    public PostgresMasterRecurringSchedulesRepository(JobMasterClusterConnectionConfig clusterConnectionConfig, IDbConnectionManager connectionManager) : 
        base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;
}
