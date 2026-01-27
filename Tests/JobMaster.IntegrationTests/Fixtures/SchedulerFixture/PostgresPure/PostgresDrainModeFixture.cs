namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.PostgresPure;

public sealed class PostgresDrainModeFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*-postgres-*";
    public override string ExcludeWildcards => "*-mixed-*;*NatsJetStream*;";
    public override string DefaultClusterId => "cluster-postgres-1";
    public override bool IsDrainingModeTest => true;
}
