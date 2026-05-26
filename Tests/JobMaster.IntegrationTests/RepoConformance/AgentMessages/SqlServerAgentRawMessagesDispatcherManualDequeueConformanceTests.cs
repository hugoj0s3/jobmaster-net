using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.AgentMessages;

[Collection("SqlServerRepositoryConformance")]
[Trait("DB", "SqlServer")]
public sealed class SqlServerAgentRawMessagesDispatcherManualDequeueConformanceTests
    : RepositoryAgentRawMessagesDispatcherManualDequeueConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerAgentRawMessagesDispatcherManualDequeueConformanceTests(SqlServerRepositoryFixture fixture) : base(fixture)
    {
    }
}
