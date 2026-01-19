namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.MySqlPure;

public sealed class MySqlDrainModeFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*cluster-mysql-1*";
    public override string ExcludeWildcards => "*-mixed-*;*natjetstream*;";
    public override string DefaultClusterId => "cluster-mysql-1";
    public override bool IsDrainingModeTest => true;
}
