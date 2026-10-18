using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ClusterIdTest.PostgresPure;

public sealed class PostgresPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : ClusterIdTestPhase1EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string DefaultClusterId => PostgresPureClusters.ClusterIdDefault.ToString().ToKebabCase();
    protected override string TargetClusterId => PostgresPureClusters.ClusterIdTarget.ToString().ToKebabCase();
    protected override string ContainerName => "pg-cluster-id";

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase1;
}
