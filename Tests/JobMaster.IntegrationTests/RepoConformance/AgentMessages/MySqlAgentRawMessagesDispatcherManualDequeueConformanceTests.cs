using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.AgentMessages;

[Collection("MySqlRepositoryConformance")]
[Trait("DB", "MySql")]
public sealed class MySqlAgentRawMessagesDispatcherManualDequeueConformanceTests
    : RepositoryAgentRawMessagesDispatcherManualDequeueConformanceTests<MySqlRepositoryFixture>
{
    public MySqlAgentRawMessagesDispatcherManualDequeueConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
