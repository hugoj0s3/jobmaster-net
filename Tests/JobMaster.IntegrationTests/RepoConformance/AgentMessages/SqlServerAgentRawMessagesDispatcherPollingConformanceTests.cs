using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.AgentMessages;

[Collection("SqlServerRepositoryConformance")]
[Trait("DB", "SqlServer")]
public sealed class SqlServerAgentRawMessagesDispatcherPollingConformanceTests
    : RepositoryAgentRawMessagesDispatcherPollingConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerAgentRawMessagesDispatcherPollingConformanceTests(SqlServerRepositoryFixture fixture) : base(fixture)
    {
    }
}
