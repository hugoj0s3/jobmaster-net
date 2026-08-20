using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Utils;
using Xunit;
using Xunit.Sdk;

namespace JobMaster.IntegrationTests.RepoConformance.AgentMessages;

// The settle delay after each push below exists for RavenDB. Its PullMessagesAsync selects candidates via
// an index-backed query with no WaitForNonStaleResults (deliberately -- see RavenDbRawMessagesDispatcherRepository;
// staleness there is safe by design, not a bug, given JobMaster's single-owner-per-bucket guarantee), so a
// push-then-immediately-pull with zero delay is genuinely flaky against RavenDB, not just slow -- confirmed
// empirically (one run missed all 3 pushed messages, another run missed 1 of 3, no delay). SQL providers
// don't need this (read-after-write is always consistent, no async indexing) -- SettleAsync is a no-op
// by default, overridden per provider that actually needs it, so only that provider's test run pays the wait.
public abstract class RepositoryAgentRawMessagesDispatcherPollingConformanceTests<TFixture>
    where TFixture : RepositoryFixtureBase
{
    protected TFixture Fixture { get; }

    protected RepositoryAgentRawMessagesDispatcherPollingConformanceTests(TFixture fixture)
    {
        Fixture = fixture;

        if (!Fixture.AgentMessages.IsPollingBased)
        {
            throw new SkipException($"{nameof(IAgentRawMessagesDispatcherRepository)} is configured for auto-dequeue.");
        }
    }

    protected virtual Task SettleAsync() => Task.CompletedTask;

    [Fact]
    public async Task CreateBucket_Push_Dequeue_ShouldRoundTrip_And_Remove()
    {
        var bucket = "polling-dequeue-" + JobMasterRandomUtil.NewGuid4();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var refTime = DateTime.UtcNow.AddSeconds(-1);
            var payload = "{\"x\":1}";
            var corrId = "c1";

            await Fixture.AgentMessages.PushMessageAsync(bucket, payload, refTime, corrId);
            await SettleAsync();

            Assert.True(await Fixture.AgentMessages.HasJobsAsync(bucket));

            var msgs = await Fixture.AgentMessages.PullMessagesAsync(bucket, 10);
            Assert.Single(msgs);

            var m = msgs[0];
            Assert.Equal(payload, m.Payload);
            Assert.Equal(corrId, m.CorrelationId);
            AssertDateTimeUtcEquivalent(refTime, m.ReferenceTime);

            Assert.False(await Fixture.AgentMessages.HasJobsAsync(bucket));
        }
        finally
        {
            await Fixture.AgentMessages.DestroyBucketAsync(bucket);
        }
    }

    [Fact]
    public async Task Dequeue_ShouldRespect_OrderByReferenceTimeThenMessageId()
    {
        var bucket = "polling-dequeue-order-" + JobMasterRandomUtil.NewGuid4();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var baseTime = DateTime.UtcNow.AddMinutes(-10);

            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":2}", baseTime.AddSeconds(2), "c2");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":1}", baseTime.AddSeconds(1), "c1");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":3}", baseTime.AddSeconds(3), "c3");
            
            await SettleAsync();

            var msgs = await Fixture.AgentMessages.PullMessagesAsync(bucket, 10);
            Assert.Equal(3, msgs.Count);

            Assert.Equal("c1", msgs[0].CorrelationId);
            Assert.Equal("c2", msgs[1].CorrelationId);
            Assert.Equal("c3", msgs[2].CorrelationId);

            Assert.False(await Fixture.AgentMessages.HasJobsAsync(bucket));
        }
        finally
        {
            await Fixture.AgentMessages.DestroyBucketAsync(bucket);
        }
    }

    [Fact]
    public async Task Dequeue_ShouldSupport_ReferenceTimeTo_Filter()
    {
        var bucket = "polling-dequeue-refto-" + JobMasterRandomUtil.NewGuid4();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var now = DateTime.UtcNow;

            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"t\":\"past\"}", now.AddMinutes(-1), "past");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"t\":\"future\"}", now.AddMinutes(10), "future");
            await SettleAsync();

            var msgs = await Fixture.AgentMessages.PullMessagesAsync(bucket, 10, referenceTimeTo: now);
            Assert.Single(msgs);
            Assert.Equal("past", msgs[0].CorrelationId);

            Assert.True(await Fixture.AgentMessages.HasJobsAsync(bucket));

            var remaining = await Fixture.AgentMessages.PullMessagesAsync(bucket, 10);
            Assert.Single(remaining);
            Assert.Equal("future", remaining[0].CorrelationId);

            Assert.False(await Fixture.AgentMessages.HasJobsAsync(bucket));
        }
        finally
        {
            await Fixture.AgentMessages.DestroyBucketAsync(bucket);
        }
    }

    [Fact]
    public async Task BulkPush_Then_Dequeue_ShouldReturn_AllMessages()
    {
        var bucket = "polling-dequeue-bulk-" + JobMasterRandomUtil.NewGuid4();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var baseTime = DateTime.UtcNow.AddMinutes(-5);
            var messages = new List<(string payload, DateTime referenceTime, string correlationId)>
            {
                ("{\"b\":1}", baseTime.AddSeconds(1), "b1"),
                ("{\"b\":2}", baseTime.AddSeconds(2), "b2"),
                ("{\"b\":3}", baseTime.AddSeconds(3), "b3"),
            };

            await Fixture.AgentMessages.BulkPushMessageAsync(bucket, messages);
            await SettleAsync();

            Assert.True(await Fixture.AgentMessages.HasJobsAsync(bucket));

            var dequeued = await Fixture.AgentMessages.PullMessagesAsync(bucket, 10);
            Assert.Equal(3, dequeued.Count);

            Assert.Equal(new[] { "b1", "b2", "b3" }, dequeued.Select(x => x.CorrelationId).ToArray());

            Assert.False(await Fixture.AgentMessages.HasJobsAsync(bucket));
        }
        finally
        {
            await Fixture.AgentMessages.DestroyBucketAsync(bucket);
        }
    }

    private static void AssertDateTimeUtcEquivalent(DateTime expectedUtc, DateTime actualUtc)
    {
        var expected = DateTime.SpecifyKind(expectedUtc, DateTimeKind.Utc);
        var actual = DateTime.SpecifyKind(actualUtc, DateTimeKind.Utc);
        var diff = (expected - actual).Duration();

        Assert.True(diff <= TimeSpan.FromMilliseconds(600), $"Expected {expected:O} but was {actual:O} (diff={diff.TotalMilliseconds}ms)");
    }
}
