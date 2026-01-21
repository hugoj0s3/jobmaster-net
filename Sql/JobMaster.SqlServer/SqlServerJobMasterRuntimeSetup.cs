using JobMaster.Sql;

namespace JobMaster.SqlServer;

internal class SqlServerJobMasterRuntimeSetup : SqlJobMasterRuntimeSetup
{
    protected override int DefaultDbOperationThrottleLimitForCluster => 250;
    protected override int DefaultDbOperationThrottleLimitForAgent => 50;
    public override string RepositoryTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
}
