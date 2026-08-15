using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.IntegrationTests.RepoConformance.GenericRecords;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

[Collection("RavenDbRepoConformance")]
[Trait("DB", "RavenDb")]
public sealed class RavenDbGenericRecordsRepositoryConformanceTests
    : RepositoryGenericRecordsConformanceTests<RavenDbRepositoryFixture>
{
    public RavenDbGenericRecordsRepositoryConformanceTests(RavenDbRepositoryFixture fixture) : base(fixture)
    {
    }
}
