using JobMaster.SqlBase;

namespace JobMaster.MySql;

internal class MySqlJobMasterRuntimeSetup : SqlJobMasterRuntimeSetup
{
    protected override int DefaultDbOperationThrottleLimitForCluster => 5;
    protected override int DefaultDbOperationThrottleLimitForAgent => 5;
    public override string RepositoryTypeId => MySqlRepositoryConstants.RepositoryTypeId;
}
