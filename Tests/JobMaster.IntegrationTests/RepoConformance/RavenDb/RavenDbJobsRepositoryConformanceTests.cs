using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.IntegrationTests.RepoConformance.Jobs;
using JobMaster.Sdk.Utils;
using Raven.Client.Exceptions;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

[Collection("RavenDbRepoConformance")]
[Trait("DB", "RavenDb")]
public sealed class RavenDbJobsRepositoryConformanceTests
    : RepositoryJobsConformanceTests<RavenDbRepositoryFixture>
{
    public RavenDbJobsRepositoryConformanceTests(RavenDbRepositoryFixture fixture) : base(fixture)
    {
    }

    // RavenDB indexes asynchronously after a write -- production code deliberately doesn't wait for this
    // (see RavenDbMasterJobsRepository's own comments), so tests that write then immediately query need
    // to accommodate the lag themselves. See the base class's SettleAsync doc comment.
    protected override Task SettleAsync() => Task.Delay(TimeSpan.FromMilliseconds(250));

    // Not a test of RavenDbMasterJobsRepository -- a direct test of RavenDB's own SaveChangesAsync
    // contract, since a planned redesign of BulkUpdateAsync(IList<JobRawModel>) (single shared session +
    // fallback to per-row retry only when the batch itself throws ConcurrencyException) depends entirely
    // on SaveChangesAsync being genuinely all-or-nothing per session. If RavenDB instead applied some
    // writes before hitting the conflicting one, the fallback's "retry every pending item from its
    // original expected version" would be unsafe: an item that actually committed during the "failed"
    // batch would fail its retry (server version already moved on) and be wrongly excluded from the
    // result despite having succeeded.
    [Fact]
    public async Task SaveChangesAsync_WithMixedValidAndStaleChangeVectors_ShouldRollBackEntireBatch()
    {
        var store = Fixture.DocumentStoreManager.GetOrCreateStore(Fixture.ConnectionString);
        var ids = Enumerable.Range(0, 5)
            .Select(i => $"atomicity-test/{JobMasterRandomUtil.NewGuid4():N}")
            .ToList();

        using (var seedSession = store.OpenAsyncSession())
        {
            foreach (var id in ids)
            {
                await seedSession.StoreAsync(new AtomicityTestDoc { Value = "original" }, id);
            }

            await seedSession.SaveChangesAsync();
        }

        string?[] changeVectors;
        using (var readSession = store.OpenAsyncSession())
        {
            var docs = await readSession.LoadAsync<AtomicityTestDoc>(ids);
            changeVectors = ids.Select(id => readSession.Advanced.GetChangeVectorFor(docs[id])).ToArray();
        }

        // Batch: 4 valid CAS writes + 1 deliberately-stale one, in the same session/SaveChangesAsync call.
        using (var batchSession = store.OpenAsyncSession())
        {
            for (var i = 0; i < ids.Count; i++)
            {
                var expectedChangeVector = i == ids.Count - 1 ? "garbage-stale-change-vector" : changeVectors[i]!;
                await batchSession.StoreAsync(new AtomicityTestDoc { Value = "updated" }, changeVector: expectedChangeVector, id: ids[i]);
            }

            await Assert.ThrowsAsync<ConcurrencyException>(() => batchSession.SaveChangesAsync());
        }

        // If SaveChangesAsync is genuinely all-or-nothing, every document should still read "original" --
        // none of the 4 valid writes should have landed just because they had correct change vectors.
        using (var verifySession = store.OpenAsyncSession())
        {
            var docs = await verifySession.LoadAsync<AtomicityTestDoc>(ids);
            foreach (var id in ids)
            {
                Assert.Equal("original", docs[id]!.Value);
            }
        }
    }

    private sealed class AtomicityTestDoc
    {
        public string Value { get; set; } = string.Empty;
    }
}
