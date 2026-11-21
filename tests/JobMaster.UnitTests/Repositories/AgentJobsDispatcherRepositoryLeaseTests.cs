using FluentAssertions;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Repositories;
using JobMaster.Sdk.Utils;
using Moq;

namespace JobMaster.UnitTests.Repositories;

public class AgentJobsDispatcherRepositoryLeaseTests
{
    private const string BucketId = "b1";

    [Fact]
    public async Task LeaseSavePendingJobsAsync_MovesMessagesAtTheLeaseLimitToTheTerminatedBucket_AndReturnsTheRest()
    {
        var raw = new Mock<IAgentRawMessagesDispatcherRepository>();
        raw.SetupGet(r => r.IsPollingBased).Returns(true);
        raw.Setup(r => r.LeaseMessagesAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<TimeSpan>()))
            .ReturnsAsync(new List<JobMasterRawMessage>
            {
                Message("exhausted", JobMasterConstants.MaxSavePendingLeaseCount),
                Message("fine", JobMasterConstants.MaxSavePendingLeaseCount - 1),
            });
        var dispatcher = new HybridJobsDispatcherRepository(null!, "test", raw.Object, raw.Object, Mock.Of<IJobMasterLogger>());

        var leased = await dispatcher.LeaseSavePendingJobsAsync(BucketId, 10, TimeSpan.FromMinutes(1));

        var address = FullBucketAddressIdsUtil.GetJobSavePendingBucketAddress(BucketId);
        var terminated = FullBucketAddressIdsUtil.GetTerminatedBucketAddress(address);
        leased.Select(x => x.AckKey).Should().Equal("fine");
        raw.Verify(r => r.CreateBucketIfNotExistsAsync(terminated), Times.Once);
        raw.Verify(r => r.UpdateBucketAsync(address, It.Is<IList<string>>(ids => ids.SequenceEqual(new[] { "exhausted" })), terminated), Times.Once);
    }

    private static JobMasterRawMessage Message(string messageId, int leaseCount)
    {
        var job = new JobRawModel { Id = JobMasterRandomUtil.NewGuid4(), BucketId = BucketId };
        return JobMasterRawMessage.RecoverFromDb(new JobMasterRawMessagePersistenceRecord
        {
            MessageId = messageId,
            Payload = InternalJobMasterSerializer.Serialize(job),
            CorrelationId = job.Id.ToString("N"),
            LeaseCount = leaseCount,
        });
    }
}
