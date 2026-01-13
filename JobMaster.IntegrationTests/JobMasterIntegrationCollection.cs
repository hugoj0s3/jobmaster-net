using JobMaster.IntegrationTests.Fixtures;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture;
using Xunit;

namespace JobMaster.IntegrationTests;

[CollectionDefinition("JobMasterIntegration", DisableParallelization = true)]
public class JobMasterIntegrationCollection : ICollectionFixture<JobMasterBaseSchedulerFixture>
{
}
