using JobMaster.SqlBase;

namespace JobMaster.MySql;

internal class MySqlJobMasterRuntimeSetup : SqlJobMasterRuntimeSetup
{
    protected override int DefaultDbOperationThrottleLimitForCluster => 20;
    protected override int DefaultDbOperationThrottleLimitForAgent => 10;
    public override string RepositoryTypeId => MySqlRepositoryConstants.RepositoryTypeId;
}
