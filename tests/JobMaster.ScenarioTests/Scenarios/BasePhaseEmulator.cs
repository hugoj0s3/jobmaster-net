using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios;

public abstract class BasePhaseEmulator<TPhaseEnum> : IPhaseEmulator
    where TPhaseEnum : struct, Enum
{
    protected readonly ScenarioGlobalEnvironment Global;
    protected readonly ScenarioRunner Runner;

    protected BasePhaseEmulator(ScenarioGlobalEnvironment global, ScenarioRunner runner)
    {
        Global = global;
        Runner = runner;
    }

    public int PhaseInt() => (int)(object)Phase();

    public abstract TPhaseEnum Phase();

    public abstract Task RunAsync();
}