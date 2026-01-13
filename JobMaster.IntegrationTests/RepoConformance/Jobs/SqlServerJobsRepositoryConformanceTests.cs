using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Jobs;

[Collection("SqlServerRepositoryConformance")]
public sealed class SqlServerJobsRepositoryConformanceTests
    : RepositoryJobsConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerJobsRepositoryConformanceTests(SqlServerRepositoryFixture fixture) : base(fixture)
    {
    }
}
