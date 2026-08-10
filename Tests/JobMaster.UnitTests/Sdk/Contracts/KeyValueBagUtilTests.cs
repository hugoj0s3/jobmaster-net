using FluentAssertions;
using JobMaster;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Jobs;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class KeyValueBagUtilTests
{
    // SetJson/GetJson route through the framework-configured JobMasterMessageJson serializer, which
    // is normally set up during host startup -- initialize it here so a bare unit test can exercise it.
    static KeyValueBagUtilTests()
    {
        JobMasterMessageJson.SetMessageSerializerIfNotSet(new DefaultJobMasterMessageJsonSerializer());
    }

    private enum TestColor
    {
        Red,
        Green,
        Blue
    }

    private class TestPayload
    {
        public string Name { get; set; } = string.Empty;
        public int Count { get; set; }
    }

    [Fact]
    public void Serialize_Metadata_ThenDeserializeMetadata_ShouldRoundTrip_AllSupportedTypes()
    {
        var guid = Guid.Parse("8e8fd3b4-1c3b-4a2b-9d86-3c28b7c7f7b1");
        var dt = new DateTime(2025, 06, 15, 10, 30, 0, DateTimeKind.Utc);

        IWritableMetadata metadata = WritableMetadata.New()
            .SetStringValue("str", "hello")
            .SetIntValue("int", 42)
            .SetLongValue("long", 9999999999L)
            .SetShortValue("short", (short)123)
            .SetByteValue("byte", (byte)7)
            .SetCharValue("char", 'Z')
            .SetBoolValue("bool", true)
            .SetDoubleValue("double", 3.14159)
            .SetDecimalValue("decimal", 123.456m)
            .SetDateTimeValue("dt", dt)
            .SetGuidValue("guid", guid);

        var json = KeyValueBagUtil.Serialize(metadata);
        var recovered = KeyValueBagUtil.DeserializeMetadata(json).ToReadable();

        recovered.GetStringValue("str").Should().Be("hello");
        recovered.GetIntValue("int").Should().Be(42);
        recovered.GetLongValue("long").Should().Be(9999999999L);
        recovered.GetShortValue("short").Should().Be((short)123);
        recovered.GetByteValue("byte").Should().Be((byte)7);
        recovered.GetCharValue("char").Should().Be('Z');
        recovered.GetBoolValue("bool").Should().BeTrue();
        recovered.GetDoubleValue("double").Should().Be(3.14159);
        recovered.GetDecimalValue("decimal").Should().Be(123.456m);
        recovered.GetDateTimeValue("dt").Should().Be(dt);
        recovered.GetDateTimeValue("dt").Kind.Should().Be(DateTimeKind.Utc);
        recovered.GetGuidValue("guid").Should().Be(guid);
    }

    [Fact]
    public void Serialize_NullMetadata_ShouldReturnEmptyJsonObject()
    {
        KeyValueBagUtil.Serialize((IWritableMetadata?)null).Should().Be("{}");
    }

    [Fact]
    public void DeserializeMetadata_NullOrEmptyString_ShouldReturnEmptyMetadata()
    {
        var recovered = KeyValueBagUtil.DeserializeMetadata(null).ToReadable();

        recovered.ToDictionary().Should().BeEmpty();
    }

    [Fact]
    public void Serialize_MessageData_ThenDeserializeMessageData_ShouldRoundTrip_AllSupportedTypes()
    {
        var guid = Guid.Parse("9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d");
        var dt = new DateTime(2025, 03, 10, 08, 15, 0, DateTimeKind.Utc);

        IWriteableMessageData msgData = new MessageData()
            .SetStringValue("str", "world")
            .SetIntValue("int", 7)
            .SetLongValue("long", 123456789L)
            .SetShortValue("short", (short)321)
            .SetByteValue("byte", (byte)9)
            .SetCharValue("char", 'Q')
            .SetBoolValue("bool", false)
            .SetDoubleValue("double", 2.71828)
            .SetDecimalValue("decimal", 987.654m)
            .SetDateTimeValue("dt", dt)
            .SetGuidValue("guid", guid)
            .SetEnumValue("color", TestColor.Green);

        var json = KeyValueBagUtil.Serialize(msgData);
        var recovered = KeyValueBagUtil.DeserializeMessageData(json).ToReadable();

        recovered.GetStringValue("str").Should().Be("world");
        recovered.GetIntValue("int").Should().Be(7);
        recovered.GetLongValue("long").Should().Be(123456789L);
        recovered.GetShortValue("short").Should().Be((short)321);
        recovered.GetByteValue("byte").Should().Be((byte)9);
        recovered.GetCharValue("char").Should().Be('Q');
        recovered.GetBoolValue("bool").Should().BeFalse();
        recovered.GetDoubleValue("double").Should().Be(2.71828);
        recovered.GetDecimalValue("decimal").Should().Be(987.654m);
        recovered.GetDateTimeValue("dt").Should().Be(dt);
        recovered.GetDateTimeValue("dt").Kind.Should().Be(DateTimeKind.Utc);
        recovered.GetGuidValue("guid").Should().Be(guid);
        recovered.GetEnumValue<TestColor>("color").Should().Be(TestColor.Green);
    }

    [Fact]
    public void Serialize_MessageData_ThenDeserializeMessageData_ByteArray_DoesNotRoundTrip()
    {
        // Documents a real, known limitation (not asserting desired behavior): System.Text.Json
        // encodes byte[] as a base64 string, and the JSON round-trip has no way to distinguish that
        // from an ordinary string on the way back in -- same ambiguity that got byte[] support
        // removed from Metadata entirely. MessageData still exposes SetByteArrayValue/GetByteArrayValue,
        // but only for callers that never serialize in between (e.g. same-process use before the
        // MsgData ever becomes a persisted JSON string) -- once serialized, GetByteArrayValue throws.
        IWriteableMessageData msgData = new MessageData()
            .SetByteArrayValue("blob", new byte[] { 1, 2, 3, 255 });

        var json = KeyValueBagUtil.Serialize(msgData);
        var recovered = KeyValueBagUtil.DeserializeMessageData(json).ToReadable();

        var act = () => recovered.GetByteArrayValue("blob");
        act.Should().Throw<KeyNotFoundException>();
    }

    [Fact]
    public void Serialize_MessageData_ThenDeserializeMessageData_Json_ShouldRoundTrip()
    {
        // SetJson stores the value pre-serialized as an ordinary string (JobMasterMessageJson.Serialize
        // under the hood), so it's a plain string value as far as the outer KeyValueBagUtil round-trip
        // is concerned -- unlike SetByteArrayValue, it's not relying on type info System.Text.Json
        // discards for a "raw" value.
        IWriteableMessageData msgData = new MessageData()
            .SetJson("payload", new TestPayload { Name = "abc", Count = 5 });

        var json = KeyValueBagUtil.Serialize(msgData);
        var recovered = KeyValueBagUtil.DeserializeMessageData(json).ToReadable();

        var payload = recovered.GetJson<TestPayload>("payload");
        payload.Name.Should().Be("abc");
        payload.Count.Should().Be(5);
    }

    [Fact]
    public void Serialize_NullMessageData_ShouldReturnEmptyJsonObject()
    {
        KeyValueBagUtil.Serialize((IWriteableMessageData?)null).Should().Be("{}");
    }

    [Fact]
    public void DeserializeMessageData_NullOrEmptyString_ShouldReturnEmptyMessageData()
    {
        var recovered = KeyValueBagUtil.DeserializeMessageData(null).ToReadable();

        recovered.ToDictionary().Should().BeEmpty();
    }
}
