namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.SqlServerPure;

public sealed class SqlServerDrainModeFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*-sqlserver-*";
    public override string ExcludeWildcards => "*-mixed-*;*natsjetstream*;";
    public override string DefaultClusterId => "cluster-sqlserver-1";
    public override bool IsDrainingModeTest => true;
}
