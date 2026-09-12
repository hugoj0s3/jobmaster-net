using FluentAssertions;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios;

/// <summary>
/// Shared behavior for the auth scenarios (NoAuth/UserPass/ApiKey/Jwt): each declares multiple
/// standalone clusters sharing one database, then this proves — under whatever auth the concrete
/// scenario's api.json configures — that the api can list every registered cluster, and that a job
/// scheduled directly against each cluster's own container can be scheduled and read back via the
/// api. Concrete subclasses only need to supply Phase() and, for the Jwt scenario, JwtSubject.
/// </summary>
public abstract class AuthApiPhase1EmulatorBase<TClusterEnum, TPhaseEnum>(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : BasePhaseEmulator<TPhaseEnum>(global, runner)
    where TClusterEnum : struct, Enum
    where TPhaseEnum : struct, Enum
{
    /// <summary>Non-null only for the Jwt scenario — fetches a bearer token before calling the api.</summary>
    protected virtual string? JwtSubject => null;

    public override async Task RunAsync()
    {
        var clusterIds = Enum.GetValues<TClusterEnum>()
            .Select(c => c.ToString()!.ToKebabCase())
            .ToList();

        var bearerToken = JwtSubject != null
            ? await Runner.Api!.GetJwtTokenAsync(JwtSubject)
            : null;

        var listedClusters = await Runner.Api!.GetClusterIdsAsync(bearerToken);
        listedClusters.Should().Contain(clusterIds);

        foreach (var clusterId in clusterIds)
        {
            var testIdentifier = Guid.NewGuid().ToString("N");

            // Each container in this scenario owns exactly one cluster and is named after it, so
            // scheduling must go directly to that container — the YARP proxy round-robins across
            // all schedule-app backends, which would misroute across distinct clusters.
            var scheduled = await Runner.ScheduleFor(clusterId)
                .ScheduleAsync("fast", testIdentifier, qtyJobs: 1, clusterId: clusterId);
            scheduled.JobIds.Should().HaveCount(1);

            await Runner.Tracker.WaitForAsync(testIdentifier, expectedCount: 1, timeout: TimeSpan.FromSeconds(30));

            var apiJob = await Runner.Api!.GetJobAsync(clusterId, scheduled.JobIds[0], bearerToken);
            apiJob.Should().NotBeNull();
        }
    }
}
