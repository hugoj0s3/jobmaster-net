using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RecurringSchedules;

[Collection("PostgresRepositoryConformance")]
public sealed class PostgresRecurringSchedulesRepositoryConformanceTests
    : RepositoryRecurringSchedulesConformanceTests<PostgresRepositoryFixture>
{
    public PostgresRecurringSchedulesRepositoryConformanceTests(PostgresRepositoryFixture fixture) : base(fixture)
    {
    }
}
