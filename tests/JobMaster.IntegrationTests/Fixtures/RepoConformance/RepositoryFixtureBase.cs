using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

public abstract class RepositoryFixtureBase : IAsyncLifetime
{
    internal abstract string ClusterId { get; set; }
    internal abstract AgentConnectionId AgentConnectionId { get; set; }

    internal abstract IMasterJobsRepository MasterJobs { get; set; }
    internal abstract IMasterRecurringSchedulesRepository MasterRecurringSchedules { get; set; }
    internal abstract IMasterGenericRecordRepository MasterGenericRecords { get; set; }
    internal abstract IMasterDistributedLockerRepository MasterDistributedLocker { get; set; }
    internal abstract IMasterLogsRepository MasterLogs { get; set; }

    internal abstract IAgentRawMessagesDispatcherRepository AgentMessages { get;set;  }
    public abstract Task InitializeAsync();

    // Disposal of the shared runtime and all 3 databases is centralized in RepoConformanceBootstrap,
    // since JobMasterRuntime is a process-wide singleton all 3 provider fixtures share.
    public virtual Task DisposeAsync() => Task.CompletedTask;
}
