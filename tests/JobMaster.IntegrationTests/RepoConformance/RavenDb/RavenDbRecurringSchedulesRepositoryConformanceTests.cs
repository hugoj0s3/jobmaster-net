using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.IntegrationTests.RepoConformance.RecurringSchedules;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

[Collection("RavenDbRepoConformance")]
[Trait("DB", "RavenDb")]
public sealed class RavenDbRecurringSchedulesRepositoryConformanceTests
    : RepositoryRecurringSchedulesConformanceTests<RavenDbRepositoryFixture>
{
    public RavenDbRecurringSchedulesRepositoryConformanceTests(RavenDbRepositoryFixture fixture) : base(fixture)
    {
    }

    // See the base class's SettleAsync doc comment.
    protected override Task SettleAsync() => Task.Delay(TimeSpan.FromMilliseconds(250));
}
