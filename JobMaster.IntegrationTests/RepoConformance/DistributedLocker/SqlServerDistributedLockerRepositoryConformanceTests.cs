using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.DistributedLocker;

[Collection("SqlServerRepositoryConformance")]
public sealed class SqlServerDistributedLockerRepositoryConformanceTests
    : RepositoryDistributedLockerConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerDistributedLockerRepositoryConformanceTests(SqlServerRepositoryFixture fixture) : base(fixture)
    {
    }
}
