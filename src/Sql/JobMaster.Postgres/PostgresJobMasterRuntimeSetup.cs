using JobMaster.Sql;

namespace JobMaster.Postgres;

internal class PostgresJobMasterRuntimeSetup : SqlJobMasterRuntimeSetup
{
    protected override int DefaultDbOperationThrottleLimitForCluster => 250;
    protected override int DefaultDbOperationThrottleLimitForAgent => 50;
    public override string RepositoryTypeId => PostgresRepositoryConstants.RepositoryTypeId;
    
}