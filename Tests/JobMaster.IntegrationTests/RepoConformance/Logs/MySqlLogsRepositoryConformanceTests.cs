using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Logs;

[Collection("RepoConformance")]
[Trait("DB", "MySql")]
public sealed class MySqlLogsRepositoryConformanceTests
    : RepositoryLogsConformanceTests<MySqlRepositoryFixture>
{
    public MySqlLogsRepositoryConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
