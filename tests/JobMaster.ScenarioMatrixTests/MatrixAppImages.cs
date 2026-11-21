using System.Text.Json;
using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioMatrixTests;

/// <summary>
/// Builds every app image the matrix scenarios use, in parallel, before the first test: a fixture's setup isn't
/// counted in any test's time, so each flavor's time is its own (a slow flavor is that provider, not a build).
/// The images come from the <c>dockerfilePath</c> of every scenario JSON, so a new scenario needs nothing here.
/// </summary>
public sealed class MatrixAppImages : IAsyncLifetime
{
    public async Task InitializeAsync()
    {
        var scenarios = Path.Combine(RepoRootLocator.Find(), "tests", "JobMaster.ScenarioMatrixTests", "Scenarios");
        var dockerfiles = Directory.EnumerateFiles(scenarios, "*.json", SearchOption.AllDirectories)
            .Select(ReadDockerfilePath)
            .OfType<string>()
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        await Task.WhenAll(dockerfiles.Select(ScenarioGlobalEnvironment.BuildAppImageOnceAsync));
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    private static string? ReadDockerfilePath(string jsonFile)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(jsonFile));
        return document.RootElement.ValueKind == JsonValueKind.Object
               && document.RootElement.TryGetProperty("dockerfilePath", out var path)
            ? path.GetString()
            : null;
    }
}
