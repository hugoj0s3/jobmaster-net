using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.GenericRecords;

[Collection("PostgresRepositoryConformance")]
[Trait("DB", "Postgres")]
public sealed class PostgresGenericRecordsRepositoryConformanceTests
    : RepositoryGenericRecordsConformanceTests<PostgresRepositoryFixture>
{
    public PostgresGenericRecordsRepositoryConformanceTests(PostgresRepositoryFixture fixture) : base(fixture)
    {
    }
}
