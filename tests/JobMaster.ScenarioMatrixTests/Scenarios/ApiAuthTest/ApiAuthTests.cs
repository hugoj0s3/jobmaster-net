using FluentAssertions;
using JobMaster.ScenarioMatrixTests.Flavors;
using JobMaster.ScenarioMatrixTests.Lib;

namespace JobMaster.ScenarioMatrixTests.Scenarios.ApiAuthTest;

/// <summary>
/// Under each API auth mode, the API lists every registered cluster, and a job scheduled on each cluster's own
/// container is read back through it. The axis here is the auth mode, not the provider. See scenario.md.
/// </summary>
[Collection(MatrixCollection.Name)]
public sealed class ApiAuthTests(ScenarioGlobalEnvironment global)
{
    // The api container's "auth" block per mode; {{ApiKey}} / {{ApiUsername}} / {{ApiPassword}} are the runner's
    // per-run secrets, attached to Runner.Api's client automatically.
    private static readonly IReadOnlyDictionary<string, string> AuthConfigs = new Dictionary<string, string>
    {
        ["noauth"] = """{ "requireAuthentication": false }""",
        ["userpass"] = """{ "requireAuthentication": true, "username": "{{ApiUsername}}", "password": "{{ApiPassword}}" }""",
        ["apikey"] = """{ "requireAuthentication": true, "apiKey": "{{ApiKey}}" }""",
        ["jwt"] = """{ "requireAuthentication": true, "enableJwt": true }""",
    };

    private const string JwtSubject = "scenario-tester";

    public static IEnumerable<object[]> Variants() =>
        ScenarioFlavors.Only(ScenarioFlavors.PostgresPure)
            .SelectMany(flavor => AuthConfigs.Keys.Select(auth => new object[] { flavor.Name, auth }));

    [Theory]
    [MemberData(nameof(Variants))]
    public async Task Run(string flavor, string auth)
    {
        var selected = ScenarioFlavors.Get(flavor);
        var folder = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios", "ApiAuthTest");

        await using var runner = await ScenarioRunner.StartFromFolderAsync(
            global, folder, json => FlavorTemplateRenderer.Render(
                json.Replace("\"{{authConfig}}\"", AuthConfigs[auth]).Replace("{{auth}}", auth), selected));
        await runner.StartPhaseAsync("phase-1");

        var api = runner.Api ?? throw new InvalidOperationException("This scenario has no api container configured.");
        var clusterIds = new[] { $"mx-{selected.Code}-{auth}-one", $"mx-{selected.Code}-{auth}-two" };

        var bearerToken = auth == "jwt" ? await api.GetJwtTokenAsync(JwtSubject) : null;

        (await api.GetClusterIdsAsync(bearerToken)).Should().Contain(clusterIds);

        foreach (var clusterId in clusterIds)
        {
            var testIdentifier = Guid.NewGuid().ToString("N");

            // Each container owns one cluster and is named after it: schedule on it directly (the proxy round-robins
            // across every schedule-app container, which would misroute between distinct clusters).
            var scheduled = await runner.ScheduleFor(clusterId).ScheduleAsync("fast", testIdentifier, qtyJobs: 1, clusterId: clusterId);
            scheduled.JobIds.Should().HaveCount(1);

            await runner.Tracker.WaitForAsync(testIdentifier, expectedCount: 1, timeout: TimeSpan.FromSeconds(30));

            (await api.GetJobAsync(clusterId, scheduled.JobIds[0], bearerToken)).Should().NotBeNull();
        }
    }
}
