using JobMaster.SqlBase;

namespace JobMaster.SqlServer;

internal class SqlServerJobMasterRuntimeSetup : SqlJobMasterRuntimeSetup
{
    protected override int DefaultDbOperationThrottleLimitForCluster => 50;
    protected override int DefaultDbOperationThrottleLimitForAgent => 25;
    public override string RepositoryTypeId => SqlServerRepositoryConstants.RepositoryTypeId;
}
