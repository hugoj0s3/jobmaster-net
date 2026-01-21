using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sql.Connections;
using JobMaster.Sql.Master;
using Npgsql;

namespace JobMaster.Postgres.Master;

internal class PostgresMasterJobsRepository : SqlMasterJobsRepository
{
    public PostgresMasterJobsRepository(JobMasterClusterConnectionConfig clusterConnectionConfig, IDbConnectionManager connectionManager) : 
        base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => PostgresRepositoryConstants.RepositoryTypeId;
    protected override bool IsDupeViolation(Guid jobId, Exception ex)
    {
        return ex is PostgresException pgEx && pgEx.SqlState == "23505";
    }
}
