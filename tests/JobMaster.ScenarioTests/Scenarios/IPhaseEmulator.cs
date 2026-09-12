namespace JobMaster.ScenarioTests.Scenarios;

public interface IPhaseEmulator
{
    Task RunAsync();
    int PhaseInt();
}