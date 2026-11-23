using System.Text.Json;
using FluentAssertions;
using JobMaster.Abstractions.Serialization;
using JobMaster.Serialization;

namespace JobMaster.UnitTests.Serialization;

public class SystemTextJsonMessageSerializerTests
{
    private class OrderPayload
    {
        public int OrderId { get; set; }
        public string Customer { get; set; } = string.Empty;
    }

    // Stands in for a user's "new format" serializer (e.g. Newtonsoft camelCase).
    private sealed class CamelCaseSerializer : IJobMasterMessageJsonSerializer
    {
        private static readonly JsonSerializerOptions Options = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        public string Serialize<T>(T obj) => JsonSerializer.Serialize(obj, Options);
        public T Deserialize<T>(string json) => JsonSerializer.Deserialize<T>(json, Options)!;
        public object Deserialize(string json, Type type) => JsonSerializer.Deserialize(json, type, Options)!;
    }

    private static readonly OrderPayload Order = new() { OrderId = 42, Customer = "acme" };

    [Fact]
    public void RejectUnknownProperties_DoesNotChangeWrites()
    {
        var lenient = new SystemTextJsonMessageSerializer();
        var strict = new SystemTextJsonMessageSerializer(rejectUnknownProperties: true);

        strict.Serialize(Order).Should().Be(lenient.Serialize(Order)).And.Be("{\"OrderId\":42,\"Customer\":\"acme\"}");
    }

    [Fact]
    public void Default_ReadsOtherFormatSilentlyAsEmptyObject()
    {
        // Why the strict option exists: the lenient default doesn't throw on camelCase JSON.
        var camelJson = new CamelCaseSerializer().Serialize(Order);

        var read = new SystemTextJsonMessageSerializer().Deserialize<OrderPayload>(camelJson);

        read.OrderId.Should().Be(0);
        read.Customer.Should().BeEmpty();
    }

    [Fact]
    public void RejectUnknownProperties_ThrowsOnOtherFormat_AndStillReadsOwnFormat()
    {
        var strict = new SystemTextJsonMessageSerializer(rejectUnknownProperties: true);
        var camelJson = new CamelCaseSerializer().Serialize(Order);

        var act = () => strict.Deserialize<OrderPayload>(camelJson);

        act.Should().Throw<JsonException>();
        strict.Deserialize<OrderPayload>(strict.Serialize(Order)).Should().BeEquivalentTo(Order);
    }

    [Fact]
    public void Fallback_Step1Node_ReadsNewFormatThroughStrictOldSerializer()
    {
        // Rolling deploy, step 2 in progress: a step-1 node (old first, strict) reads JSON a step-2 node wrote.
        var step1 = new FallbackMessageJsonSerializer(
            new SystemTextJsonMessageSerializer(rejectUnknownProperties: true),
            new CamelCaseSerializer());
        var writtenByStep2Node = new CamelCaseSerializer().Serialize(Order);

        step1.Deserialize<OrderPayload>(writtenByStep2Node).Should().BeEquivalentTo(Order);
        step1.Deserialize<OrderPayload>(step1.Serialize(Order)).Should().BeEquivalentTo(Order);
    }
}
