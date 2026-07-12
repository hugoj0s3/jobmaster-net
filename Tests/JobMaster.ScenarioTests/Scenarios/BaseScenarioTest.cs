using JobMaster.ScenarioTests.Fixtures;
using Xunit;

namespace JobMaster.ScenarioTests.Scenarios;

/// <summary>
/// Reusable xUnit wiring for a scenario emulator: loads it, runs every phase in order, tears it
/// down. A concrete scenario test is just a one-line subclass naming its emulator/cluster/phase
/// enum types — see Scenarios/BasicExecution/BasicExecutionTests.cs.
/// </summary>
[Collection(ScenarioCollection.Name)]
public abstract class BaseScenarioTest<TEmulator, TClusterEnum, TPhaseEnum> : IAsyncLifetime
    where TEmulator : BaseScenarioEmulator<TClusterEnum, TPhaseEnum>
    where TClusterEnum : Enum
    where TPhaseEnum : struct, Enum
{
    private readonly ScenarioGlobalEnvironment global;

    protected TEmulator Emulator { get; private set; } = default!;

    protected BaseScenarioTest(ScenarioGlobalEnvironment global)
    {
        this.global = global;
    }

    public async Task InitializeAsync()
    {
        Emulator = (TEmulator)Activator.CreateInstance(typeof(TEmulator), global)!;
        await Emulator.InitializeAsync();
    }

    public async Task DisposeAsync()
    {
        await Emulator.DisposeAsync();
    }

    [Fact]
    public Task RunAllPhases() => Emulator.RunAllPhasesAsync();
}
