using System.Text.Json.Nodes;
using FluentAssertions;

namespace JobMaster.ScenarioMatrixTests.Flavors;

// No Docker: checks only how a template is filled per flavor.
public sealed class FlavorTemplateRendererTests
{
    private const string Template = """
        {
          "containerName": "mx-{{flavor}}-basic",
          "clusterConfigTemplates": [
            {
              "ClusterId": "mx-{{flavor}}-basic",
              "RepoType": "{{master:RepoType}}",
              "ConnectionString": "{{master:ConnectionString:BasicCluster}}",
              "AgentConnections": [
                { "Name": "agent-1", "Connection": "{{agent:1}}" },
                { "Name": "agent-2", "Connection": "{{agent:2}}" }
              ]
            }
          ]
        }
        """;

    [Fact]
    public void SingleProviderFlavor_FillsMasterAndEachAgentSlot_KeepingTheNames()
    {
        var cluster = RenderCluster(ScenarioFlavors.PostgresNats);

        cluster["ClusterId"]!.GetValue<string>().Should().Be("mx-pgnats-basic");
        cluster["RepoType"]!.GetValue<string>().Should().Be("Postgres");
        cluster["ConnectionString"]!.GetValue<string>().Should().Contain("Database=MxPgnatsBasicCluster;");

        var agent = cluster["AgentConnections"]![0]!.AsObject();
        agent["Name"]!.GetValue<string>().Should().Be("agent-1");
        agent["RepositoryType"]!.GetValue<string>().Should().Be("NatsJetStream");
        agent.ContainsKey("Connection").Should().BeFalse();
        agent.ContainsKey("Transport").Should().BeFalse();
    }

    [Fact]
    public void HybridFlavor_ExpandsSlotsIntoTransportAndExecution_InBothDirections()
    {
        var agents = RenderCluster(ScenarioFlavors.HybridNatsRavenDb)["AgentConnections"]!.AsArray();

        agents[0]!["Name"]!.GetValue<string>().Should().Be("agent-1");
        agents[0]!["Transport"]!["RepositoryType"]!.GetValue<string>().Should().Be("NatsJetStream");
        agents[0]!["Execution"]!["RepositoryType"]!.GetValue<string>().Should().Be("RavenDB");
        agents[1]!["Transport"]!["RepositoryType"]!.GetValue<string>().Should().Be("RavenDB");
        agents[1]!["Execution"]!["RepositoryType"]!.GetValue<string>().Should().Be("NatsJetStream");
        agents[0]!.AsObject().ContainsKey("RepositoryType").Should().BeFalse();
    }

    [Fact]
    public void PureFlavor_GivesEveryAgentSlotItsOwnDatabase()
    {
        var agents = RenderCluster(ScenarioFlavors.MySqlPure)["AgentConnections"]!.AsArray();

        agents[0]!["ConnectionString"]!.GetValue<string>().Should().Contain("Database=MxMyAgent1;");
        agents[1]!["ConnectionString"]!.GetValue<string>().Should().Contain("Database=MxMyAgent2;");
    }

    [Fact]
    public void InfrastructureTokens_AreLeftForTheRunner()
    {
        var rendered = FlavorTemplateRenderer.Render(Template, ScenarioFlavors.SqlServerPure);

        rendered.Should().Contain("{{SqlServerHost}}").And.NotContain("{{flavor}}").And.NotContain("{{agent:");
    }

    [Fact]
    public void ConnectionWithoutAnAgentPlaceholder_Throws()
    {
        var act = () => FlavorTemplateRenderer.Render("""{ "Name": "agent-1", "Connection": "Host=x" }""", ScenarioFlavors.PostgresPure);

        act.Should().Throw<InvalidOperationException>().WithMessage("*{{agent:N}}*");
    }

    [Fact]
    public void AllExcept_DropsOnlyTheExcludedFlavors_AndNeedsAReason()
    {
        var names = ScenarioFlavors.AllExcept((ScenarioFlavors.HybridNatsRavenDb, "needs one provider per slot"))
            .Select(flavor => flavor.Name).ToList();

        names.Should().HaveCount(ScenarioFlavors.All.Count - 1).And.NotContain("HybridNatsRavenDb");

        var noReason = () => ScenarioFlavors.AllExcept((ScenarioFlavors.PostgresNats, " "));
        noReason.Should().Throw<ArgumentException>().WithMessage("*PostgresNats*reason*");
    }

    [Fact]
    public void Get_UnknownFlavor_Throws()
    {
        var act = () => ScenarioFlavors.Get("Oracle");

        act.Should().Throw<ArgumentException>().WithMessage("*Oracle*");
    }

    private static JsonObject RenderCluster(ScenarioFlavor flavor)
    {
        var root = JsonNode.Parse(FlavorTemplateRenderer.Render(Template, flavor))!;
        return root["clusterConfigTemplates"]![0]!.AsObject();
    }
}
