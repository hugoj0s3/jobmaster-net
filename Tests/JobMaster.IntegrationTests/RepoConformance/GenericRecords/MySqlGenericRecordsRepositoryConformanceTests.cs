using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.GenericRecords;

[Collection("MySqlRepositoryConformance")]
[Trait("DB", "MySql")]
public sealed class MySqlGenericRecordsRepositoryConformanceTests
    : RepositoryGenericRecordsConformanceTests<MySqlRepositoryFixture>
{
    public MySqlGenericRecordsRepositoryConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
