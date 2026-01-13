using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Jobs;

[Collection("MySqlRepositoryConformance")]
public sealed class MySqlJobsRepositoryConformanceTests
    : RepositoryJobsConformanceTests<MySqlRepositoryFixture>
{
    public MySqlJobsRepositoryConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
