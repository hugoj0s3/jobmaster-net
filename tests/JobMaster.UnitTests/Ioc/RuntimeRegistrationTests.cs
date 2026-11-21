using FluentAssertions;
using JobMaster.Postgres;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Ioc.Setup;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Ioc;

public class RuntimeRegistrationTests
{
    [Fact]
    public void Runtime_IsRegisteredOnceByFactory_SoTheAppContainerDisposesIt()
    {
        // Disposing the runtime stops the workers; the container only disposes what it created, so an instance
        // registration would silently lose that stop.
        var services = new ServiceCollection();
        foreach (var clusterId in new[] { "RtReg-a-" + Guid.NewGuid().ToString("N")[..8], "RtReg-b-" + Guid.NewGuid().ToString("N")[..8] })
        {
            var builder = new ClusterConfigBuilder(clusterId, services);
            builder.UsePostgresForMaster("Host=localhost;Database=master;Username=u;Password=p");
            builder.Finish();
        }

        var runtime = services.Where(d => d.ServiceType == typeof(IJobMasterRuntime)).ToList();

        runtime.Should().ContainSingle();
        runtime[0].ImplementationFactory.Should().NotBeNull();
        runtime[0].ImplementationInstance.Should().BeNull();
    }
}
