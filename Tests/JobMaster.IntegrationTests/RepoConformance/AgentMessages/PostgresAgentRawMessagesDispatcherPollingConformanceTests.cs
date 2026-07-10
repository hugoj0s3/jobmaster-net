using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.AgentMessages;

[Collection("PostgresRepositoryConformance")]
[Trait("DB", "Postgres")]
public sealed class PostgresAgentRawMessagesDispatcherPollingConformanceTests
    : RepositoryAgentRawMessagesDispatcherPollingConformanceTests<PostgresRepositoryFixture>
{
    public PostgresAgentRawMessagesDispatcherPollingConformanceTests(PostgresRepositoryFixture fixture) : base(fixture)
    {
    }
}
