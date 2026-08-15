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
}
