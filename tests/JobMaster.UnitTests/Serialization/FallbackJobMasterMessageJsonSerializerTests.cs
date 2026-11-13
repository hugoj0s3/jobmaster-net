using FluentAssertions;
using JobMaster.Abstractions.Serialization;
using JobMaster.Serialization;

namespace JobMaster.UnitTests.Serialization;

public class FallbackJobMasterMessageJsonSerializerTests
{
    private sealed class FakeSerializer : IJobMasterMessageJsonSerializer
    {
        private readonly string name;
        private readonly bool throws;

        public FakeSerializer(string name, bool throws = false)
        {
            this.name = name;
            this.throws = throws;
        }

        public int ReadCalls { get; private set; }

        public string Serialize<T>(T obj) => name;

        public T Deserialize<T>(string json) => (T)Deserialize(json, typeof(T));

        public object Deserialize(string json, Type type)
        {
            ReadCalls++;
            if (throws) throw new FormatException($"bad payload {json}");
            return name;
        }
    }

    [Fact]
    public void Serialize_UsesPrimary()
    {
        var serializer = new FallbackJobMasterMessageJsonSerializer(new FakeSerializer("primary"), new FakeSerializer("fallback"));

        serializer.Serialize(new object()).Should().Be("primary");
    }

    [Fact]
    public void Deserialize_WhenPrimarySucceeds_DoesNotTryFallback()
    {
        var fallback = new FakeSerializer("fallback");
        var serializer = new FallbackJobMasterMessageJsonSerializer(new FakeSerializer("primary"), fallback);

        serializer.Deserialize<string>("{}").Should().Be("primary");
        fallback.ReadCalls.Should().Be(0);
    }

    [Fact]
    public void Deserialize_WhenEarlierReadersThrow_ReturnsFirstSuccessInOrder()
    {
        var serializer = new FallbackJobMasterMessageJsonSerializer(
            new FakeSerializer("primary", throws: true),
            new FakeSerializer("first", throws: true),
            new FakeSerializer("second"),
            new FakeSerializer("third"));

        serializer.Deserialize<string>("{}").Should().Be("second");
        serializer.Deserialize("{}", typeof(string)).Should().Be("second");
    }

    [Fact]
    public void Deserialize_WhenAllThrow_ThrowsWithoutPayload()
    {
        var serializer = new FallbackJobMasterMessageJsonSerializer(
            new FakeSerializer("primary", throws: true),
            new FakeSerializer("fallback", throws: true));

        var generic = () => serializer.Deserialize<string>("secret-payload");
        var byType = () => serializer.Deserialize("secret-payload", typeof(string));

        generic.Should().Throw<InvalidOperationException>()
            .Which.Message.Should().Contain(nameof(FormatException)).And.NotContain("secret-payload");
        generic.Should().Throw<InvalidOperationException>().Which.InnerException.Should().BeNull();
        byType.Should().Throw<InvalidOperationException>().Which.Message.Should().NotContain("secret-payload");
    }

    [Fact]
    public void Constructor_RejectsNulls()
    {
        var valid = new FakeSerializer("valid");

        var nullPrimary = () => new FallbackJobMasterMessageJsonSerializer(null!, valid);
        var nullArray = () => new FallbackJobMasterMessageJsonSerializer(valid, null!);
        var nullEntry = () => new FallbackJobMasterMessageJsonSerializer(valid, valid, null!);

        nullPrimary.Should().Throw<ArgumentNullException>();
        nullArray.Should().Throw<ArgumentNullException>();
        nullEntry.Should().Throw<ArgumentNullException>();
    }
}
