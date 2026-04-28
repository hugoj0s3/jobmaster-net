namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.SqlServerPure;

public sealed class SqlServerPureFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*-sqlserver-*";
    public override string ExcludeWildcards => "*-mixed-*;*Nats*;";
    public override string DefaultClusterId => "cluster-sqlserver-1";
}