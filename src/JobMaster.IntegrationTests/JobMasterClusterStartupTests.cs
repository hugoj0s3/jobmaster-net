using JobMaster.IntegrationTests.Fixtures;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Services.Master;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.IntegrationTests;

[Collection("JobMasterIntegration")]
public class JobMasterClusterStartupTests
{
    private readonly JobMasterBaseSchedulerFixture fixture;

    public JobMasterClusterStartupTests(JobMasterBaseSchedulerFixture fixture)
    {
        this.fixture = fixture;
    }

    private void SkipIfNotConfigured()
    {
        if (!fixture.IsConfigured)
        {
            throw new Xunit.Sdk.SkipException(fixture.NotConfiguredReason ?? "Integration tests are not configured.");
        }
    }

    [Fact]
    public void Runtime_ShouldBeStarted_AndDefaultClusterShouldBeResolved()
    {
        SkipIfNotConfigured();
        Assert.True(JobMasterRuntimeSingleton.Instance.Started);
        Assert.NotNull(JobMasterClusterConnectionConfig.Default);
        Assert.Equal(fixture.DefaultClusterId, JobMasterClusterConnectionConfig.Default!.ClusterId);
    }

    [Fact]
    public void CanResolve_MasterClusterConfigurationService_FromClusterFactory()
    {
        SkipIfNotConfigured();
        var factory = JobMasterClusterAwareComponentFactories.GetFactory(fixture.DefaultClusterId);
        var cfgSvc = factory.GetComponent<IMasterClusterConfigurationService>();
        Assert.NotNull(cfgSvc.Get());
    }
}
