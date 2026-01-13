namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.Mixed;

public sealed class MixedSchedulerFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*-mixed-*";
    public override string ExcludeWildcards => string.Empty;
    
    public override string DefaultClusterId => "cluster-mixed-postgres-1";
}