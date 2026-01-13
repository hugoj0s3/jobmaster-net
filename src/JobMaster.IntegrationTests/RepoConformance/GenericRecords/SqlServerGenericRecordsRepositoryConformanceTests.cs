using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.GenericRecords;

[Collection("SqlServerRepositoryConformance")]
public sealed class SqlServerGenericRecordsRepositoryConformanceTests
    : RepositoryGenericRecordsConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerGenericRecordsRepositoryConformanceTests(SqlServerRepositoryFixture fixture) : base(fixture)
    {
    }
}
