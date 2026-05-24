using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.AgentMessages;

[Collection("PostgresRepositoryConformance")]
[Trait("DB", "Postgres")]
public sealed class PostgresAgentRawMessagesDispatcherManualDequeueConformanceTests
    : RepositoryAgentRawMessagesDispatcherManualDequeueConformanceTests<PostgresRepositoryFixture>
{
    public PostgresAgentRawMessagesDispatcherManualDequeueConformanceTests(PostgresRepositoryFixture fixture) : base(fixture)
    {
    }
}
