using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.AgentMessages;

[Collection("RepoConformance")]
[Trait("DB", "MySql")]
public sealed class MySqlAgentRawMessagesDispatcherPollingConformanceTests
    : RepositoryAgentRawMessagesDispatcherPollingConformanceTests<MySqlRepositoryFixture>
{
    public MySqlAgentRawMessagesDispatcherPollingConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
