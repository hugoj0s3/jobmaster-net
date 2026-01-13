using JobMaster.Sql;

namespace JobMaster.MySql;

internal class MySqlJobMasterRuntimeSetup : SqlJobMasterRuntimeSetup
{
    protected override int DefaultDbOperationThrottleLimitForCluster => 125;
    protected override int DefaultDbOperationThrottleLimitForAgent => 25;
    public override string RepositoryTypeId => MySqlRepositoryConstants.RepositoryTypeId;
}
