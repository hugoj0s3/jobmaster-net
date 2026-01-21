using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sql.Connections;
using JobMaster.Sql.Master;
using MySqlConnector;

namespace JobMaster.MySql.Master;

internal class MySqlMasterJobsRepository : SqlMasterJobsRepository
{
    public MySqlMasterJobsRepository(
        JobMasterClusterConnectionConfig clusterConnectionConfig,
        IDbConnectionManager connectionManager) : base(clusterConnectionConfig, connectionManager)
    {
    }

    public override string MasterRepoTypeId => MySqlRepositoryConstants.RepositoryTypeId;
    protected override bool IsDupeViolation(Guid jobId, Exception ex)
    {
        if (ex is MySqlException mysqlEx)
        {
            // Strict: only treat canonical ER_DUP_ENTRY as duplication
            return mysqlEx.Number == 1062
                   || mysqlEx.ErrorCode == MySqlErrorCode.DuplicateKeyEntry;
        }
        return false; // no inner recursion to avoid false positives
    }
}
