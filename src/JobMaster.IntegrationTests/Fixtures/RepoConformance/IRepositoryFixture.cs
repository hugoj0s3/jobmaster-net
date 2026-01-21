using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

public interface IRepositoryFixture : IAsyncLifetime
{
    string ClusterId { get; }
    AgentConnectionId AgentConnectionId { get; }

    IServiceProvider Services { get; }

    IMasterJobsRepository MasterJobs { get; }
    IMasterRecurringSchedulesRepository MasterRecurringSchedules { get; }
    IMasterGenericRecordRepository MasterGenericRecords { get; }
    IMasterDistributedLockerRepository MasterDistributedLocker { get; }

    IAgentRawMessagesDispatcherRepository AgentMessages { get; }
}
