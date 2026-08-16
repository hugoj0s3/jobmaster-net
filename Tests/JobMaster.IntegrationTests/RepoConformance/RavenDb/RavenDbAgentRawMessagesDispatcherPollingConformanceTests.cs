using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.IntegrationTests.RepoConformance.AgentMessages;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

[Collection("RavenDbRepoConformance")]
[Trait("DB", "RavenDb")]
public sealed class RavenDbAgentRawMessagesDispatcherPollingConformanceTests
    : RepositoryAgentRawMessagesDispatcherPollingConformanceTests<RavenDbRepositoryFixture>
{
    public RavenDbAgentRawMessagesDispatcherPollingConformanceTests(RavenDbRepositoryFixture fixture) : base(fixture)
    {
    }

    // See the base class's SettleAsync doc comment -- RavenDB's PullMessagesAsync deliberately
    // doesn't wait for index freshness, so push-then-immediately-pull tests need to accommodate that lag.
    protected override Task SettleAsync() => Task.Delay(TimeSpan.FromMilliseconds(250));
}
