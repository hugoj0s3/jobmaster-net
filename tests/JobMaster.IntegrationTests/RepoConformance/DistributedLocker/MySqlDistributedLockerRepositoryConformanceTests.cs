using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.DistributedLocker;

[Collection("RepoConformance")]
[Trait("DB", "MySql")]
public sealed class MySqlDistributedLockerRepositoryConformanceTests
    : RepositoryDistributedLockerConformanceTests<MySqlRepositoryFixture>
{
    public MySqlDistributedLockerRepositoryConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
