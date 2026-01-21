using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.DistributedLocker;

[Collection("MySqlRepositoryConformance")]
public sealed class MySqlDistributedLockerRepositoryConformanceTests
    : RepositoryDistributedLockerConformanceTests<MySqlRepositoryFixture>
{
    public MySqlDistributedLockerRepositoryConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
