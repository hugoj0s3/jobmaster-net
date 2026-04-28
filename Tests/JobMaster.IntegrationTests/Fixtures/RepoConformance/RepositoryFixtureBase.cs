using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;

namespace JobMaster.IntegrationTests.Fixtures.RepoConformance;

public abstract class RepositoryFixtureBase : IAsyncLifetime
{
    internal abstract string ClusterId { get; set; }
    internal abstract AgentConnectionId AgentConnectionId { get; set; }

    internal abstract IServiceProvider Services { get; set; }

    internal abstract IMasterJobsRepository MasterJobs { get; set; }
    internal abstract IMasterRecurringSchedulesRepository MasterRecurringSchedules { get; set; }
    internal abstract IMasterGenericRecordRepository MasterGenericRecords { get; set; }
    internal abstract IMasterDistributedLockerRepository MasterDistributedLocker { get; set; }

    internal abstract IAgentRawMessagesDispatcherRepository AgentMessages { get;set;  }
    public abstract Task InitializeAsync();
    public async Task DisposeAsync()
    {
        if (Services is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync();
        else if (Services is IDisposable disposable)
            disposable.Dispose();
    }
}
