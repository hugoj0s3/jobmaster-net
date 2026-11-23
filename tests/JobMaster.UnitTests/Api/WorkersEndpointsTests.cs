using FluentAssertions;
using JobMaster.Api.ApiModels;
using JobMaster.Api.Endpoints;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.UnitTests.Api;

public class WorkersEndpointsTests
{
    [Fact]
    public void FilterWorkers_WorkerLane_IgnoresCase()
    {
        var billing = Worker("w1", "Billing");
        var other = Worker("w2", "Other");

        var filtered = WorkersEndpoints.FilterWorkers(new List<AgentWorkerModel> { billing, other }, new ApiAgentWorkerCriteria { WorkerLane = "billing" });

        filtered.Should().ContainSingle().Which.Should().BeSameAs(billing);
    }

    private static AgentWorkerModel Worker(string id, string lane) => new("cluster")
    {
        Id = id,
        Name = id,
        AgentConnectionId = new AgentConnectionId("cluster", "agent"),
        WorkerLane = lane,
    };
}
