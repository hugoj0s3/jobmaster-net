using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios.ApiAuths.Jwt;

public sealed class JwtPhase1Emulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    : AuthApiPhase1EmulatorBase<JwtClusters, JwtPhases>(global, runner)
{
    public override JwtPhases Phase() => JwtPhases.Phase1;

    protected override string? JwtSubject => "scenario-tester";
}
