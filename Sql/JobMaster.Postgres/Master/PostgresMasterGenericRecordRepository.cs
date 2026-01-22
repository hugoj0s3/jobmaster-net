using JobMaster.Sdk.Abstractions.Config;
using JobMaster.SqlBase.Connections;
using JobMaster.SqlBase.Master;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterGenericRecordRepository : SqlMasterGenericRecordRepository
{
    public PostgresMasterGenericRecordRepository
        (JobMasterClusterConnectionConfig clusterConnectionConfig, IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;
}
