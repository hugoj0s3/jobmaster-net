using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.IntegrationTests.RepoConformance.DistributedLocker;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

[Collection("RavenDbRepoConformance")]
[Trait("DB", "RavenDb")]
public sealed class RavenDbDistributedLockerRepositoryConformanceTests
    : RepositoryDistributedLockerConformanceTests<RavenDbRepositoryFixture>
{
    public RavenDbDistributedLockerRepositoryConformanceTests(RavenDbRepositoryFixture fixture) : base(fixture)
    {
    }
}
