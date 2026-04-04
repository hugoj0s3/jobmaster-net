namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.MySqlPure;

public sealed class MySqlPureSchedulerFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*-mysql-*";
    public override string ExcludeWildcards => "*-mixed-*;*Nats*;";
    public override string DefaultClusterId => "cluster-mysql-1";
}