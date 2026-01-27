namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.MySqlPure;

public sealed class MySqlPureSchedulerFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*-mysql-*";
    public override string ExcludeWildcards => "*-mixed-*;*natsjetstream*;";
    public override string DefaultClusterId => "cluster-mysql-1";
}