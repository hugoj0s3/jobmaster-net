using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.DistributedLocker;

[Collection("PostgresRepositoryConformance")]
public sealed class PostgresDistributedLockerRepositoryConformanceTests
    : RepositoryDistributedLockerConformanceTests<PostgresRepositoryFixture>
{
    public PostgresDistributedLockerRepositoryConformanceTests(PostgresRepositoryFixture fixture) : base(fixture)
    {
    }
}
