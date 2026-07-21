using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Jobs;

[Collection("RepoConformance")]
[Trait("DB", "Postgres")]
public sealed class PostgresJobsRepositoryConformanceTests
    : RepositoryJobsConformanceTests<PostgresRepositoryFixture>
{
    public PostgresJobsRepositoryConformanceTests(PostgresRepositoryFixture fixture) : base(fixture)
    {
    }
}
