using Docker.DotNet.Models;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;

namespace JobMaster.ScenarioMatrixTests.Lib;

internal static class ContainerStopper
{
    /// <summary>
    /// Stops <paramref name="container"/> the way <paramref name="stopType"/> says. A crash is a SIGKILL sent by
    /// Docker from outside: a <c>kill -9 1</c> run inside the container is ignored (PID 1 doesn't get signals it has
    /// no handler for), so it can't simulate one.
    /// </summary>
    public static async Task StopAsync(IContainer container, ScenarioStopType stopType, CancellationToken ct = default)
    {
        if (stopType == ScenarioStopType.SigTerm)
        {
            await container.StopAsync(ct);
            return;
        }

        using var docker = TestcontainersSettings.OS.DockerEndpointAuthConfig
            .GetDockerClientConfiguration(ResourceReaper.DefaultSessionId)
            .CreateClient();
        await docker.Containers.KillContainerAsync(container.Id, new ContainerKillParameters { Signal = "SIGKILL" }, ct);
    }
}
