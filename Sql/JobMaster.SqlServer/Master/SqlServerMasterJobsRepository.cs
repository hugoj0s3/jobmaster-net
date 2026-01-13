using JobMaster.Sql.Connections;
using JobMaster.Sql.Master;
using JobMaster.Sdk.Contracts.Config;
using Microsoft.Data.SqlClient;

namespace JobMaster.SqlServer.Master;

internal class SqlServerMasterJobsRepository : SqlMasterJobsRepository
{
    public SqlServerMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
    protected override bool IsDupeViolation(Guid jobId, Exception ex)
    {
        return ex is SqlException sqlEx && sqlEx.Number == 2627;
    }
}
