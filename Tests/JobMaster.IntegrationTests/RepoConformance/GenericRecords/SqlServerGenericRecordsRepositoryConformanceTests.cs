using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.GenericRecords;

[Collection("SqlServerRepositoryConformance")]
[Trait("DB", "SqlServer")]
public sealed class SqlServerGenericRecordsRepositoryConformanceTests
    : RepositoryGenericRecordsConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerGenericRecordsRepositoryConformanceTests(SqlServerRepositoryFixture fixture) : base(fixture)
    {
    }
}
