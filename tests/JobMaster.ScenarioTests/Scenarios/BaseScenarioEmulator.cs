using JobMaster.ScenarioTests.Fixtures;
using JobMaster.ScenarioTests.Runner;

namespace JobMaster.ScenarioTests.Scenarios;

public abstract class BaseScenarioEmulator<TClusterEnum, TPhaseEnum>
    where TClusterEnum : Enum
    where TPhaseEnum : struct, Enum
{
    private readonly ScenarioGlobalEnvironment global;

    private ScenarioRunner? runner;

    public IReadOnlyCollection<IPhaseEmulator> PhaseEmulators { get; private set; } = Array.Empty<IPhaseEmulator>();

    protected BaseScenarioEmulator(ScenarioGlobalEnvironment global)
    {
        this.global = global;
    }

    /// <summary>Everything in TClusterEnum's namespace after "...Scenarios." is a folder — e.g.
    /// JobMaster.ScenarioTests.Scenarios.ApiAuths.NoAuth -> Scenarios/ApiAuths/NoAuth.</summary>
    private const string ScenariosNamespacePrefix = "JobMaster.ScenarioTests.Scenarios.";

    public async Task InitializeAsync()
    {
        var ns = typeof(TClusterEnum).Namespace ?? "";
        var scenarioPath = ns.StartsWith(ScenariosNamespacePrefix, StringComparison.Ordinal)
            ? ns[ScenariosNamespacePrefix.Length..].Replace('.', '/')
            : ns;

        runner = await ScenarioRunner.StartAsync(global, scenarioPath);

        PhaseEmulators = CreatePhaseEmulators();
    }

    public async Task RunPhaseAsync(TPhaseEnum phase)
    {
        if (runner is null)
            throw new InvalidOperationException("Call InitializeAsync before running a phase.");

        var phaseEmulator = PhaseEmulators.FirstOrDefault(x => x.PhaseInt() == (int)(object)phase);
        if (phaseEmulator is null)
            throw new InvalidOperationException($"Phase {phase} is not supported.");

        // Container lifecycle is the emulator's job, not each phase's — a phase emulator only
        // implements what happens once its containers are up (schedule, assert, kill, etc.).
        await runner.StartPhaseAsync($"phase-{phaseEmulator.PhaseInt()}");

        await phaseEmulator.RunAsync();
    }

    /// <summary>Runs every phase in order (by PhaseInt). The common case: one scenario, one test.</summary>
    public async Task RunAllPhasesAsync()
    {
        foreach (var phaseEmulator in PhaseEmulators)
        {
            await RunPhaseAsync((TPhaseEnum)(object)phaseEmulator.PhaseInt());
        }
    }

    private IReadOnlyList<IPhaseEmulator> CreatePhaseEmulators()
    {
        if (runner is null)
            throw new InvalidOperationException("Call InitializeAsync before creating phase emulators.");

        var phaseBaseType = typeof(BasePhaseEmulator<TPhaseEnum>);
        var assembly = typeof(BaseScenarioEmulator<TClusterEnum, TPhaseEnum>).Assembly;

        return assembly.GetTypes()
            .Where(type =>
                type is { IsAbstract: false, IsInterface: false } &&
                phaseBaseType.IsAssignableFrom(type))
            .Select(type =>
            {
                var instance = Activator.CreateInstance(type, global, runner);
                return instance as IPhaseEmulator
                       ?? throw new InvalidOperationException(
                           $"Type {type.Name} does not implement {nameof(IPhaseEmulator)} correctly.");
            })
            .OrderBy(x => x.PhaseInt())
            .ToList();
    }

    public async Task DisposeAsync()
    {
        if (runner != null)
            await runner.DisposeAsync();
    }
}