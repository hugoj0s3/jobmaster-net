using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Logs;

[Collection("RepoConformance")]
[Trait("DB", "SqlServer")]
public sealed class SqlServerLogsRepositoryConformanceTests
    : RepositoryLogsConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerLogsRepositoryConformanceTests(SqlServerRepositoryFixture fixture) : base(fixture)
    {
    }
}
