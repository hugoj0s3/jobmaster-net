using DotNet.Testcontainers.Builders;
using FluentAssertions;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioMatrixTests.Flavors;

// Real Docker, a few seconds: a shell whose SIGTERM handler exits with 0 tells the two stop types apart.
public sealed class ContainerStopperTests
{
    [Theory]
    [InlineData(ScenarioStopType.SigTerm, 0)]   // the handler ran: the app's own shutdown would too
    [InlineData(ScenarioStopType.Crash, 137)]   // 128 + SIGKILL: ended before running anything
    public async Task StopAsync_EndsTheProcessTheWayTheStopTypeSays(ScenarioStopType stopType, long expectedExitCode)
    {
        await using var container = new ContainerBuilder()
            .WithImage("alpine:3.20")
            .WithEntrypoint("sh", "-c", "trap 'exit 0' TERM; echo ready; sleep 600 & wait")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("ready"))
            .WithCleanUp(true)
            .Build();
        await container.StartAsync();

        await ContainerStopper.StopAsync(container, stopType);

        (await container.GetExitCodeAsync()).Should().Be(expectedExitCode);
    }
}
