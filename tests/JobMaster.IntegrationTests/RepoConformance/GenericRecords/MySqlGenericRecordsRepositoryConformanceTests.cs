using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.GenericRecords;

[Collection("RepoConformance")]
[Trait("DB", "MySql")]
public sealed class MySqlGenericRecordsRepositoryConformanceTests
    : RepositoryGenericRecordsConformanceTests<MySqlRepositoryFixture>
{
    public MySqlGenericRecordsRepositoryConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
