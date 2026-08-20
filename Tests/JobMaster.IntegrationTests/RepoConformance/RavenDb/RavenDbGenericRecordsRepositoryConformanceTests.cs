using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.IntegrationTests.RepoConformance.GenericRecords;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Utils;
using Xunit;

namespace JobMaster.IntegrationTests.RepoConformance.RavenDb;

[Collection("RavenDbRepoConformance")]
[Trait("DB", "RavenDb")]
public sealed class RavenDbGenericRecordsRepositoryConformanceTests
    : RepositoryGenericRecordsConformanceTests<RavenDbRepositoryFixture>
{
    public RavenDbGenericRecordsRepositoryConformanceTests(RavenDbRepositoryFixture fixture) : base(fixture)
    {
    }

    // Regression test: GenericRecordEntry.ToStorageObject's JobMasterConfigDictionary case returns the
    // raw IDictionary<string, object> itself, not pre-JSON-encoded -- unlike the SQL providers' own EAV
    // storage, which always flattens any non-scalar value to JSON text before persisting, RavenDB's
    // Values field would otherwise store this dictionary as a natively-nested JSON object. On read,
    // GenericRecordEntry.FromStorageObject's matching branch unconditionally calls stored.ToString(),
    // expecting JSON text back -- against a real Dictionary object this just returns its .NET type name,
    // not JSON, so deserialization threw. ClusterConfigurationModel.AdditionalConfig is the one
    // real-world caller of this path (every cluster persists one on startup), so a fresh instance
    // round-tripping cleanly through Upsert/Get proves the fix rather than exercising it in isolation.
    [Fact]
    public async Task Upsert_ShouldRoundTrip_RecordWithNestedDictionaryValue()
    {
        var namespaceKey = new JobMasterNamespaceUniqueKey("diagnostic-group", Guid.NewGuid());
        var model = new ClusterConfigurationModel(Fixture.ClusterId) { IanaTimeZoneId = "America/Sao_Paulo" };
        model.AdditionalConfig.SetValue(namespaceKey, "key", "value");
        var entry = GenericRecordEntry.Create(Fixture.ClusterId, MasterGenericRecordGroupIds.ClusterConfiguration, Fixture.ClusterId, model);

        await Fixture.MasterGenericRecords.UpsertAsync(entry);

        var fromDb = await Fixture.MasterGenericRecords.GetAsync(MasterGenericRecordGroupIds.ClusterConfiguration, Fixture.ClusterId);
        Assert.NotNull(fromDb);

        var restored = fromDb!.ToObject<ClusterConfigurationModel>();
        Assert.Equal(model.IanaTimeZoneId, restored.IanaTimeZoneId);
        Assert.Equal(model.ClusterMode, restored.ClusterMode);
        Assert.Equal("value", restored.AdditionalConfig.TryGetValue<string>(namespaceKey, "key"));
    }

    // RavenDB-specific: verifies an UPDATE to an EXISTING document's field (not a brand-new insert) is
    // reflected correctly by Query immediately afterward, with no artificial test-side delay -- relies
    // solely on QueryAsync's built-in WaitForNonStaleResults(). This is a different risk than "does the
    // index know this document still matches the filter" (already covered by the shared base class's
    // insert-then-query tests) -- it's specifically about whether an in-place field update on an existing
    // document is visible right away, the exact scenario a bucket status change is.
    //
    // Deliberately uses MasterGenericRecordGroupIds.Bucket, not a synthetic per-test group id: RavenDb
    // GenericRecordRepository.Query only applies WaitForNonStaleResults() for the closed set of groups
    // that back a SentinelCachedReader factory (see GroupsRequiringFreshRead in
    // RavenDbMasterGenericRecordRepository) -- a synthetic group id would silently skip the wait and this
    // test would just be passing on local-container indexing speed, not proving the guarantee.
    [Fact]
    public async Task Query_ShouldReflect_UpdatedFieldValue_OnExistingRecord_Immediately()
    {
        var groupId = MasterGenericRecordGroupIds.Bucket;
        var entryId = "bucket-" + JobMasterRandomUtil.NewGuid4().ToString("N");

        var entry = NewEntry(groupId, entryId);
        entry.Values = new Dictionary<string, object?>(StringComparer.Ordinal) { ["Status"] = "Active" };
        await Fixture.MasterGenericRecords.UpsertAsync(entry);

        var baseline = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria { IncludeExpired = true });
        Assert.Single(baseline);
        Assert.Equal("Active", baseline[0].Values["Status"]);

        // Update the SAME document (not a new insert) -- this is the scenario in question.
        entry.Values = new Dictionary<string, object?>(StringComparer.Ordinal) { ["Status"] = "Inactive" };
        await Fixture.MasterGenericRecords.UpsertAsync(entry);

        // No delay here on purpose -- QueryAsync's own WaitForNonStaleResults() is what's under test.
        var afterUpdate = await Fixture.MasterGenericRecords.QueryAsync(groupId, new GenericRecordQueryCriteria { IncludeExpired = true });
        Assert.Single(afterUpdate);
        Assert.Equal("Inactive", afterUpdate[0].Values["Status"]);
    }
}
