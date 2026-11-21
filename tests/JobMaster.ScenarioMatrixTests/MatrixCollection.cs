using JobMaster.ScenarioTests.Fixtures;

namespace JobMaster.ScenarioMatrixTests;

// xUnit v2 only picks up collection definitions from the test assembly itself, so the shared engines
// fixture is declared again here. MatrixAppImages builds the app images before the first test.
[CollectionDefinition(Name)]
public sealed class MatrixCollection : ICollectionFixture<ScenarioGlobalEnvironment>, ICollectionFixture<MatrixAppImages>
{
    public const string Name = "ScenarioMatrix";
}
