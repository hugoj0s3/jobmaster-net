namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.Mixed;

public sealed class MixedDrainModeFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*-mixed-postgres-1";
    public override string ExcludeWildcards => string.Empty;
    public override string DefaultClusterId => "cluster-mixed-postgres-1";
    public override bool IsDrainingModeTest => true;
}
