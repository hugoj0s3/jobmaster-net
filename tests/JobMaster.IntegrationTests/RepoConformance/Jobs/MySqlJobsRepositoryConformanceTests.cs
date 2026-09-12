using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;
using Xunit.Abstractions;

namespace JobMaster.IntegrationTests.RepoConformance.Jobs;

[Collection("RepoConformance")]
[Trait("DB", "MySql")]
public sealed class MySqlJobsRepositoryConformanceTests
    : RepositoryJobsConformanceTests<MySqlRepositoryFixture>
{
    public MySqlJobsRepositoryConformanceTests(MySqlRepositoryFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }
}
