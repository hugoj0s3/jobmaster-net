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

    [Fact]
    public async Task Lease_ShouldReturnMessagesInOrder_AndHideThemWhileLeased()
    {
        var bucket = "polling-lease-" + JobMasterRandomUtil.NewGuid4();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var baseTime = DateTime.UtcNow.AddMinutes(-5);
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":2}", baseTime.AddSeconds(2), "c2");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":1}", baseTime.AddSeconds(1), "c1");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":3}", baseTime.AddSeconds(3), "c3");
            await SettleAsync();

            var leased = await Fixture.AgentMessages.LeaseMessagesAsync(bucket, 2, TimeSpan.FromMinutes(1));
            Assert.Equal(new[] { "c1", "c2" }, leased.Select(x => x.CorrelationId).ToArray());

            var next = await Fixture.AgentMessages.LeaseMessagesAsync(bucket, 10, TimeSpan.FromMinutes(1));
            Assert.Equal(new[] { "c3" }, next.Select(x => x.CorrelationId).ToArray());

            Assert.Empty(await Fixture.AgentMessages.LeaseMessagesAsync(bucket, 10, TimeSpan.FromMinutes(1)));

            // Leased messages are still stored: the bucket isn't empty until they're acked.
            Assert.True(await Fixture.AgentMessages.HasJobsAsync(bucket));
        }
        finally
        {
            await Fixture.AgentMessages.DestroyBucketAsync(bucket);
        }
    }

    [Fact]
    public async Task Lease_AfterTheLeaseEnds_ShouldReturnTheMessagesAgain_CountingEachLease()
    {
        var bucket = "polling-lease-expiry-" + JobMasterRandomUtil.NewGuid4();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":1}", DateTime.UtcNow.AddSeconds(-1), "c1");
            await SettleAsync();

            var first = await Fixture.AgentMessages.LeaseMessagesAsync(bucket, 10, TimeSpan.FromSeconds(1));
            Assert.Single(first);
            Assert.Equal(1, first[0].LeaseCount);

            await Task.Delay(TimeSpan.FromSeconds(2));
            await SettleAsync();

            var again = await Fixture.AgentMessages.LeaseMessagesAsync(bucket, 10, TimeSpan.FromMinutes(1));
            Assert.Single(again);
            Assert.Equal(first[0].MessageId, again[0].MessageId);
            Assert.Equal(2, again[0].LeaseCount);
        }
        finally
        {
            await Fixture.AgentMessages.DestroyBucketAsync(bucket);
        }
    }

    [Fact]
    public async Task Ack_ShouldRemoveOnlyTheAckedMessages_AndIgnoreUnknownIds()
    {
        var bucket = "polling-ack-" + JobMasterRandomUtil.NewGuid4();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var baseTime = DateTime.UtcNow.AddMinutes(-5);
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":1}", baseTime.AddSeconds(1), "c1");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":2}", baseTime.AddSeconds(2), "c2");
            await SettleAsync();

            var leased = await Fixture.AgentMessages.LeaseMessagesAsync(bucket, 10, TimeSpan.FromSeconds(1));
            Assert.Equal(2, leased.Count);

            await Fixture.AgentMessages.AckAsync(bucket, new List<string> { leased[0].MessageId, "no-such-message" });

            // After the lease ends only the un-acked one comes back.
            await Task.Delay(TimeSpan.FromSeconds(2));
            await SettleAsync();
            var again = await Fixture.AgentMessages.LeaseMessagesAsync(bucket, 10, TimeSpan.FromMinutes(1));
            Assert.Equal(new[] { "c2" }, again.Select(x => x.CorrelationId).ToArray());

            await Fixture.AgentMessages.AckAsync(bucket, again.Select(x => x.MessageId).ToList());
            Assert.False(await Fixture.AgentMessages.HasJobsAsync(bucket));
        }
        finally
        {
            await Fixture.AgentMessages.DestroyBucketAsync(bucket);
        }
    }

    [Fact]
    public async Task UpdateBucket_ShouldMoveOnlyTheGivenMessages_Unleased()
    {
        var bucket = "polling-move-" + JobMasterRandomUtil.NewGuid4();
        var target = "JMReserved-Terminated::" + bucket;
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var baseTime = DateTime.UtcNow.AddMinutes(-5);
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":1}", baseTime.AddSeconds(1), "c1");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":2}", baseTime.AddSeconds(2), "c2");
            await SettleAsync();
            var leased = await Fixture.AgentMessages.LeaseMessagesAsync(bucket, 10, TimeSpan.FromSeconds(1));

            // Twice: the second create is a no-op.
            await Fixture.AgentMessages.CreateBucketIfNotExistsAsync(target);
            await Fixture.AgentMessages.CreateBucketIfNotExistsAsync(target);
            await Fixture.AgentMessages.UpdateBucketAsync(bucket, new List<string> { leased[0].MessageId, "no-such-message" }, target);
            await SettleAsync();

            // Moved unleased (no wait for the lease), keeping its lease count.
            var moved = await Fixture.AgentMessages.LeaseMessagesAsync(target, 10, TimeSpan.FromMinutes(1));
            Assert.Equal(new[] { "c1" }, moved.Select(x => x.CorrelationId).ToArray());
            Assert.Equal(2, moved[0].LeaseCount);

            await Task.Delay(TimeSpan.FromSeconds(2));
            await SettleAsync();
            var left = await Fixture.AgentMessages.LeaseMessagesAsync(bucket, 10, TimeSpan.FromMinutes(1));
            Assert.Equal(new[] { "c2" }, left.Select(x => x.CorrelationId).ToArray());
        }
        finally
        {
            await Fixture.AgentMessages.DestroyBucketAsync(bucket);
            await Fixture.AgentMessages.DestroyBucketAsync(target);
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
