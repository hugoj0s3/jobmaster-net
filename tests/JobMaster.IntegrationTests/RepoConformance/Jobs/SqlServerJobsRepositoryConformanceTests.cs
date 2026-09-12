using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;
using Xunit.Abstractions;

namespace JobMaster.IntegrationTests.RepoConformance.Jobs;

[Collection("RepoConformance")]
[Trait("DB", "SqlServer")]
public sealed class SqlServerJobsRepositoryConformanceTests
    : RepositoryJobsConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerJobsRepositoryConformanceTests(SqlServerRepositoryFixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }
}
