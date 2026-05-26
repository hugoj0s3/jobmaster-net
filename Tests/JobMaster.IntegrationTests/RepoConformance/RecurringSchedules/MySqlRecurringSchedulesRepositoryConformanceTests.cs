using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RecurringSchedules;

[Collection("MySqlRepositoryConformance")]
[Trait("DB", "MySql")]
public sealed class MySqlRecurringSchedulesRepositoryConformanceTests
    : RepositoryRecurringSchedulesConformanceTests<MySqlRepositoryFixture>
{
    public MySqlRecurringSchedulesRepositoryConformanceTests(MySqlRepositoryFixture fixture) : base(fixture)
    {
    }
}
