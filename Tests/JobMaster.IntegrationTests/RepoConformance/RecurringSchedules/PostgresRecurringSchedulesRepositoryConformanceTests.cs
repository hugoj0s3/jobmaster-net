using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RecurringSchedules;

[Collection("PostgresRepositoryConformance")]
[Trait("DB", "Postgres")]
public sealed class PostgresRecurringSchedulesRepositoryConformanceTests
    : RepositoryRecurringSchedulesConformanceTests<PostgresRepositoryFixture>
{
    public PostgresRecurringSchedulesRepositoryConformanceTests(PostgresRepositoryFixture fixture) : base(fixture)
    {
    }
}
