using FluentAssertions;
using JobMaster.Ioc.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace JobMaster.UnitTests.Ioc;

public class StartJobMasterRuntimeTests
{
    [Fact]
    public async Task StartJobMasterRuntimeAsync_WithoutAnyCluster_FailsWithTheNoClusterError()
    {
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        var act = () => serviceProvider.StartJobMasterRuntimeAsync();

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("No cluster configured*");
    }
}
