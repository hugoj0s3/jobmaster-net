namespace JobMaster.ScenarioTests.Runner;

public sealed class ScenarioDefinition
{
    public string ScenarioName { get; set; } = "";
    public List<PhaseDefinition> Phases { get; set; } = new();
    public List<InfrastructureDefinition> Infrastructure { get; set; } = new();
    public ApiReference? Api { get; set; }
}

public sealed class PhaseDefinition
{
    public string Name { get; set; } = "";
    public List<string> Containers { get; set; } = new();
}

public sealed class InfrastructureDefinition
{
    public string Type { get; set; } = "";
    public string Name { get; set; } = "";
}

public sealed class ApiReference
{
    public string Container { get; set; } = "";
}
