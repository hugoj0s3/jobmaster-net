using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RecurringSchedules;

[Collection("SqlServerRepositoryConformance")]
public sealed class SqlServerRecurringSchedulesRepositoryConformanceTests
    : RepositoryRecurringSchedulesConformanceTests<SqlServerRepositoryFixture>
{
    public SqlServerRecurringSchedulesRepositoryConformanceTests(SqlServerRepositoryFixture fixture) : base(fixture)
    {
    }
}
