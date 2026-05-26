using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.Jobs;

[Collection("SqlServerRepositoryConformance")]
[Trait("DB", "SqlServer")]
public sealed class SqlServerJobsRepositoryConformanceTests
    : RepositoryJobsConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerJobsRepositoryConformanceTests(SqlServerRepositoryFixture fixture) : base(fixture)
    {
    }
}
