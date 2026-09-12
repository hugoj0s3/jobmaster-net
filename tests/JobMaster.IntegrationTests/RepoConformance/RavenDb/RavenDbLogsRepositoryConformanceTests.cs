using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.IntegrationTests.RepoConformance.Logs;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

[Collection("RavenDbRepoConformance")]
[Trait("DB", "RavenDb")]
public sealed class RavenDbLogsRepositoryConformanceTests
    : RepositoryLogsConformanceTests<RavenDbRepositoryFixture>
{
    public RavenDbLogsRepositoryConformanceTests(RavenDbRepositoryFixture fixture) : base(fixture)
    {
    }

    // Production QueryAsync/CountAsync/DeleteByTimestampAsync/QueryForReferenceIdsAsync deliberately
    // don't wait for index freshness (matches SQL's actual consistency model -- read-after-write isn't
    // guaranteed any sooner than that), so a test that writes then immediately asserts needs to let
    // indexing settle first, rather than assume the very next read already reflects it.
    protected override Task SettleAsync() => Task.Delay(TimeSpan.FromSeconds(2));
}
