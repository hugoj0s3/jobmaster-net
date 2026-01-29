using JobMaster.IntegrationTests.Fixtures.RepoConformance;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using Xunit;
using Xunit.Sdk;

namespace JobMaster.IntegrationTests.RepoConformance.AgentMessages;

public abstract class RepositoryAgentRawMessagesDispatcherManualDequeueConformanceTests<TFixture>
    where TFixture : RepositoryFixtureBase
{
    protected TFixture Fixture { get; }

    protected RepositoryAgentRawMessagesDispatcherManualDequeueConformanceTests(TFixture fixture)
    {
        Fixture = fixture;

        if (Fixture.AgentMessages.IsAutoDequeue)
        {
            throw new SkipException($"{nameof(IAgentRawMessagesDispatcherRepository)} is configured for auto-dequeue.");
        }
    }

    [Fact]
    public async Task CreateBucket_Push_Dequeue_ShouldRoundTrip_And_Remove()
    {
        var bucket = "manual-dequeue-" + Guid.NewGuid();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var refTime = DateTime.UtcNow.AddSeconds(-1);
            var payload = "{\"x\":1}";
            var corrId = "c1";

            await Fixture.AgentMessages.PushMessageAsync(bucket, payload, refTime, corrId);

            Assert.True(await Fixture.AgentMessages.HasJobsAsync(bucket));

            var msgs = await Fixture.AgentMessages.DequeueMessagesAsync(bucket, 10);
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
        var bucket = "manual-dequeue-order-" + Guid.NewGuid();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var baseTime = DateTime.UtcNow.AddMinutes(-10);

            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":2}", baseTime.AddSeconds(2), "c2");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":1}", baseTime.AddSeconds(1), "c1");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"i\":3}", baseTime.AddSeconds(3), "c3");

            var msgs = await Fixture.AgentMessages.DequeueMessagesAsync(bucket, 10);
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
        var bucket = "manual-dequeue-refto-" + Guid.NewGuid();
        await Fixture.AgentMessages.CreateBucketAsync(bucket);

        try
        {
            var now = DateTime.UtcNow;

            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"t\":\"past\"}", now.AddMinutes(-1), "past");
            await Fixture.AgentMessages.PushMessageAsync(bucket, "{\"t\":\"future\"}", now.AddMinutes(10), "future");

            var msgs = await Fixture.AgentMessages.DequeueMessagesAsync(bucket, 10, referenceTimeTo: now);
            Assert.Single(msgs);
            Assert.Equal("past", msgs[0].CorrelationId);

            Assert.True(await Fixture.AgentMessages.HasJobsAsync(bucket));

            var remaining = await Fixture.AgentMessages.DequeueMessagesAsync(bucket, 10);
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
        var bucket = "manual-dequeue-bulk-" + Guid.NewGuid();
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

            Assert.True(await Fixture.AgentMessages.HasJobsAsync(bucket));

            var dequeued = await Fixture.AgentMessages.DequeueMessagesAsync(bucket, 10);
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
