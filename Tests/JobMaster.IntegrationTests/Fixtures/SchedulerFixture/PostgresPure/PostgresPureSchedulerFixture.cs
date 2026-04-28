namespace JobMaster.IntegrationTests.Fixtures.SchedulerFixture.PostgresPure;

public sealed class PostgresPureSchedulerFixture : JobMasterBaseSchedulerFixture
{
    public override string IncludeWildcards => "*-postgres-*";
    public override string ExcludeWildcards => "*-mixed-*;*nats*;";
    public override string DefaultClusterId => "cluster-postgres-1";
}