using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Logs;

[Collection("RepoConformance")]
[Trait("DB", "Postgres")]
public sealed class PostgresLogsRepositoryConformanceTests
    : RepositoryLogsConformanceTests<PostgresRepositoryFixture>
{
    public PostgresLogsRepositoryConformanceTests(PostgresRepositoryFixture fixture) : base(fixture)
    {
    }
}
