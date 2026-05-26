using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Jobs;

[Collection("MySqlRepositoryConformance")]
[Trait("DB", "MySql")]
public sealed class MySqlJobsRepositoryConformanceTests
    : RepositoryJobsConformanceTests<MySqlRepositoryFixture>
{
    public MySqlJobsRepositoryConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
