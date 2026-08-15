using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.IntegrationTests.RepoConformance.GenericRecords;
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
