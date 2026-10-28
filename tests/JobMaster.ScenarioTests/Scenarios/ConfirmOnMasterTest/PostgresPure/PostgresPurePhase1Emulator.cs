using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ConfirmOnMasterTest.PostgresPure;

public sealed class PostgresPurePhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : ConfirmOnMasterTestPhase1EmulatorBase<PostgresPurePhases>(global, runner)
{
    protected override string ClusterId => PostgresPureClusters.ConfirmDist.ToString().ToKebabCase();
    protected override string ContainerName => "pg-confirm";

    public override PostgresPurePhases Phase() => PostgresPurePhases.Phase1;
}
