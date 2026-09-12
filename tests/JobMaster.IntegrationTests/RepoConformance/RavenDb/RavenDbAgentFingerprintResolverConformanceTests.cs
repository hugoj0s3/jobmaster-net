using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.RavenDb;
using JobMaster.RavenDb.Agents;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Utils;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

// No shared "AgentFingerprintResolver" RepoConformance category exists for any provider (SQL/NATS
// included), so this is a standalone RavenDB-only test class, same reasoning as
// RavenDbLogsRepositoryConformanceTests. Constructs RavenDbAgentFingerprintResolver directly rather than
// resolving it through DI -- this fixture only registers the master side of UseRavenDb, so RegisterForAgent never
// runs and there's no agent-connection DI graph to resolve it from.
[Collection("RavenDbRepoConformance")]
[Trait("DB", "RavenDb")]
public sealed class RavenDbAgentFingerprintResolverConformanceTests
{
    private readonly RavenDbRepositoryFixture fixture;

    public RavenDbAgentFingerprintResolverConformanceTests(RavenDbRepositoryFixture fixture)
    {
        this.fixture = fixture;
    }

    private RavenDbAgentFingerprintResolver NewResolver(string agentConnectionId)
    {
        var resolver = new RavenDbAgentFingerprintResolver(fixture.DocumentStoreManager);
        resolver.Initialize(new JobMasterAgentConnectionConfig(
            fixture.ClusterId,
            agentConnectionId,
            fixture.ConnectionString,
            RavenDbRepositoryConstants.RepositoryTypeId));
        return resolver;
    }

    [Fact]
    public async Task GiveYourFingerprintAsync_ShouldReturnSameValue_OnRepeatedCalls()
    {
        var agentConnectionId = "agent-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        var resolver = NewResolver(agentConnectionId);

        var first = await resolver.GiveYourFingerprintAsync(fixture.ClusterId, agentConnectionId);
        var second = await resolver.GiveYourFingerprintAsync(fixture.ClusterId, agentConnectionId);

        Assert.False(string.IsNullOrWhiteSpace(first));
        Assert.Equal(first, second);
    }

    [Fact]
    public async Task GiveYourFingerprintAsync_ShouldReturnDifferentValues_ForDifferentAgentConnections()
    {
        var agentConnectionIdA = "agent-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        var agentConnectionIdB = "agent-" + JobMasterRandomUtil.NewGuid4().ToString("N");
        var resolver = NewResolver(agentConnectionIdA);

        var fingerprintA = await resolver.GiveYourFingerprintAsync(fixture.ClusterId, agentConnectionIdA);
        var fingerprintB = await resolver.GiveYourFingerprintAsync(fixture.ClusterId, agentConnectionIdB);

        Assert.NotEqual(fingerprintA, fingerprintB);
    }

    [Fact]
    public async Task GiveYourFingerprintAsync_ConcurrentCalls_ShouldAgreeOnSameFingerprint()
    {
        var agentConnectionId = "agent-" + JobMasterRandomUtil.NewGuid4().ToString("N");

        // Mirrors the distributed locker's mutual-exclusivity test: several independent resolver
        // instances race to be the first to create the same compare-exchange key. Exactly one write
        // should win; every caller (including the losers) must agree on the same resulting value.
        var tasks = Enumerable.Range(0, 10)
            .Select(_ => NewResolver(agentConnectionId).GiveYourFingerprintAsync(fixture.ClusterId, agentConnectionId).AsTask())
            .ToArray();

        var results = await Task.WhenAll(tasks);

        Assert.All(results, r => Assert.False(string.IsNullOrWhiteSpace(r)));
        Assert.Single(results.Distinct());
    }
}
