using FluentAssertions;
using JobMaster;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Serialization;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Serialization;

namespace JobMaster.UnitTests.Sdk.Contracts;

public class KeyValueBagUtilTests
{
    // SetJson/GetJson route through the framework-configured JobMasterMessageJson serializer, which
    // is normally set up during host startup -- initialize it here so a bare unit test can exercise it.
    static KeyValueBagUtilTests()
    {
        JobMasterMessageJson.SetMessageSerializerIfNotSet(new SystemTextJsonMessageSerializer());
    }

    private enum TestColor
    {
        Red,
        Green,
        Blue
    }

    [Flags]
    private enum TestPermissions
    {
        None = 0,
        Read = 1,
        Write = 2,
        Delete = 4
    }

    private enum TestSmallCode : byte
    {
        A = 1,
        B = 44
    }

    private class TestPayload
    {
        public string Name { get; set; } = string.Empty;
        public int Count { get; set; }
    }

    [Fact]
    public void Metadata_And_MessageData_ReadKeysIgnoringCase()
    {
        var metadata = WritableMetadata.New().SetStringValue("Category", "billing");
        var data = WriteableMessageData.New().SetIntValue("OrderId", 7);

        metadata.ToReadable().GetStringValue("CATEGORY").Should().Be("billing");
        data.ToReadable().GetIntValue("orderid").Should().Be(7);
    }

    [Fact]
    public void Set_SameKeyOtherCase_OverwritesValue_KeepsFirstSpelling()
    {
        var metadata = WritableMetadata.New()
            .SetStringValue("Category", "a")
            .SetStringValue("category", "b");

        metadata.ToDictionary().Should().Equal(new Dictionary<string, object?> { ["Category"] = "b" });
    }

    [Fact]
    public void Set_SameKeyOtherCase_KeepsKeyOrder()
    {
        var metadata = WritableMetadata.New()
            .SetStringValue("First", "1")
            .SetStringValue("Second", "2")
            .SetStringValue("Third", "3")
            .SetStringValue("FIRST", "1b");

        metadata.ToDictionary().Keys.Should().Equal("First", "Second", "Third");
    }

    [Fact]
    public void Constructor_FromCaseSensitiveDictionary_CollapsesSpellings_LastValueWins()
    {
        var stored = new Dictionary<string, object?>(StringComparer.Ordinal) { ["Category"] = "a", ["CATEGORY"] = "b" };

        var metadata = new Metadata(stored);

        metadata.ToDictionary().Should().Equal(new Dictionary<string, object?> { ["Category"] = "b" });
        metadata.ToReadable().GetStringValue("category").Should().Be("b");
    }

    [Fact]
    public void FromDictionary_CopiesTheDictionary_ForMetadataAndMessageData()
    {
        var source = new Dictionary<string, object?> { ["Key"] = "a" };

        var metadata = WritableMetadata.FromDictionary(source).SetStringValue("Other", "m");
        var data = WriteableMessageData.FromDictionary(source).SetStringValue("Other", "d");
        source["Key"] = "changed";

        source.Keys.Should().Equal("Key");
        metadata.ToReadable().GetStringValue("key").Should().Be("a");
        data.ToReadable().GetStringValue("key").Should().Be("a");
    }

    [Fact]
    public void Serialize_ThenDeserializeMetadata_KeepsKeySpelling_AndReadsIgnoringCase()
    {
        var json = KeyValueBagUtil.Serialize(WritableMetadata.New().SetStringValue("Category", "billing"));

        var metadata = KeyValueBagUtil.DeserializeMetadata(json);

        metadata.ToDictionary().Keys.Should().Equal("Category");
        metadata.ToReadable().GetStringValue("category").Should().Be("billing");
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
    public void Serialize_MessageData_ThenDeserializeMessageData_ByteArray_ShouldRoundTrip()
    {
        // System.Text.Json writes byte[] as a base64 string; GetByteArrayValue decodes it back.
        var bytes = new byte[] { 1, 2, 3, 255 };
        IWriteableMessageData msgData = new MessageData().SetByteArrayValue("blob", bytes);

        var json = KeyValueBagUtil.Serialize(msgData);
        var recovered = KeyValueBagUtil.DeserializeMessageData(json).ToReadable();

        recovered.GetByteArrayValue("blob").Should().Equal(bytes);
    }

    [Fact]
    public void Serialize_MessageData_ThenDeserializeMessageData_RandomByteArrays_ShouldRoundTrip()
    {
        // Guards the outer layer's DateTime/Guid string normalization from ever claiming a base64 value.
        var random = new Random(12345);
        for (var i = 0; i < 20_000; i++)
        {
            var bytes = new byte[random.Next(0, 48)];
            random.NextBytes(bytes);
            IWriteableMessageData msgData = new MessageData().SetByteArrayValue("blob", bytes);

            var recovered = KeyValueBagUtil.DeserializeMessageData(KeyValueBagUtil.Serialize(msgData)).ToReadable();

            recovered.GetByteArrayValue("blob").Should().Equal(bytes, $"base64 '{Convert.ToBase64String(bytes)}'");
        }
    }

    [Theory]
    [InlineData("Blue")]
    [InlineData("blue")]
    [InlineData("2")]
    public void TryGetEnumValue_WhenStringIsMember_ReturnsIt(string stored)
    {
        var msgData = new MessageData().SetStringValue("color", stored).ToReadable();

        msgData.TryGetEnumValue<TestColor>("color").Should().Be(TestColor.Blue);
    }

    [Theory]
    [InlineData("WRONG")]
    [InlineData("7")]
    [InlineData("Red, Blue")] // ORs to a member value; still not a single member of a non-[Flags] enum
    public void TryGetEnumValue_WhenStringIsNotMember_Throws(string stored)
    {
        var msgData = new MessageData().SetStringValue("color", stored).ToReadable();

        var act = () => msgData.TryGetEnumValue<TestColor>("color");

        act.Should().Throw<InvalidOperationException>();
        msgData.TryGetEnumValue<TestColor>("color", ignoreInvalidValues: true).Should().BeNull();
    }

    [Fact]
    public void TryGetEnumValue_WhenSetAsIntegerType_ReadsMemberInMemoryAndAfterPersistence()
    {
        IWriteableMessageData msgData = new MessageData()
            .SetByteValue("byte", 1).SetByteValue("byteInvalid", 7)
            .SetShortValue("short", 1).SetShortValue("shortInvalid", 7)
            .SetIntValue("int", 1).SetIntValue("intInvalid", 7)
            .SetLongValue("long", 1L).SetLongValue("longInvalid", 7L);
        var inMemory = msgData.ToReadable();
        var recovered = KeyValueBagUtil.DeserializeMessageData(KeyValueBagUtil.Serialize(msgData)).ToReadable();

        foreach (var readable in new[] { inMemory, recovered })
        {
            foreach (var key in new[] { "byte", "short", "int", "long" })
            {
                readable.TryGetEnumValue<TestColor>(key).Should().Be(TestColor.Green, key);
                var act = () => readable.TryGetEnumValue<TestColor>(key + "Invalid");
                act.Should().Throw<InvalidOperationException>(key + "Invalid");
            }
        }
    }

    [Fact]
    public void TryGetEnumValue_WhenValueIsNotIntegral_ThrowsInsteadOfRounding()
    {
        // 1.6 would round to 2 (Blue); persisted it comes back as decimal 1.6. true / 'a' would become 1 / 97.
        IWriteableMessageData msgData = new MessageData()
            .SetDoubleValue("double", 1.6)
            .SetDecimalValue("decimal", 1.6m)
            .SetBoolValue("bool", true)
            .SetCharValue("char", (char)1);
        var inMemory = msgData.ToReadable();
        var recovered = KeyValueBagUtil.DeserializeMessageData(KeyValueBagUtil.Serialize(msgData)).ToReadable();

        foreach (var readable in new[] { inMemory, recovered })
        {
            foreach (var key in new[] { "double", "decimal", "bool", "char" })
            {
                var act = () => readable.TryGetEnumValue<TestColor>(key);
                act.Should().Throw<InvalidOperationException>(key);
            }
        }
    }

    [Fact]
    public void TryGetEnumValue_WhenNumberOverflowsUnderlyingType_ThrowsInsteadOfTruncating()
    {
        // 300 truncated to a byte is 44, which is a defined member.
        IWriteableMessageData msgData = new MessageData().SetIntValue("code", 300).SetIntValue("valid", 44);
        var inMemory = msgData.ToReadable();
        var recovered = KeyValueBagUtil.DeserializeMessageData(KeyValueBagUtil.Serialize(msgData)).ToReadable();

        foreach (var readable in new[] { inMemory, recovered })
        {
            var act = () => readable.TryGetEnumValue<TestSmallCode>("code");
            act.Should().Throw<InvalidOperationException>();
            readable.TryGetEnumValue<TestSmallCode>("valid").Should().Be(TestSmallCode.B);
        }
    }

    [Fact]
    public void TryGetEnumValue_WhenPersistedNumberIsNotMember_Throws()
    {
        // SetEnumValue stores a number; a member removed in a later version comes back as an undefined one.
        IWriteableMessageData msgData = new MessageData().SetEnumValue("color", (TestColor)7);
        var recovered = KeyValueBagUtil.DeserializeMessageData(KeyValueBagUtil.Serialize(msgData)).ToReadable();

        var act = () => recovered.GetEnumValue<TestColor>("color");

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void TryGetEnumValue_WhenFlagsCombination_AcceptsDefinedBitsOnly()
    {
        var valid = new MessageData().SetEnumValue("p", TestPermissions.Read | TestPermissions.Delete).ToReadable();
        var validString = new MessageData().SetStringValue("p", "Read, Write").ToReadable();
        var unknownBit = new MessageData().SetIntValue("p", 8).ToReadable();

        valid.TryGetEnumValue<TestPermissions>("p").Should().Be(TestPermissions.Read | TestPermissions.Delete);
        validString.TryGetEnumValue<TestPermissions>("p").Should().Be(TestPermissions.Read | TestPermissions.Write);
        var act = () => unknownBit.TryGetEnumValue<TestPermissions>("p");
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void TryGetJson_WhenValueIsNotValidForType_Throws()
    {
        var msgData = new MessageData().SetStringValue("payload", "{\"Count\":\"not-a-number\"}").ToReadable();

        var act = () => msgData.TryGetJson<TestPayload>("payload");

        act.Should().Throw<InvalidOperationException>().Which.Message.Should().NotContain("not-a-number");
    }

    [Fact]
    public void TryGetJson_WhenIgnoringDeserializationErrors_ReturnsNull()
    {
        var msgData = new MessageData().SetStringValue("payload", "{\"Count\":\"not-a-number\"}").ToReadable();

        msgData.TryGetJson<TestPayload>("payload", ignoreDeserializationErrors: true).Should().BeNull();
    }

    [Theory]
    [InlineData("null")]
    [InlineData(" null ")]
    [InlineData("")]
    [InlineData("   ")]
    public void TryGetJson_WhenStoredValueIsEmptyOrJsonNull_ReturnsNull(string stored)
    {
        // A stored JSON null literal or an empty value reads as missing, not as a broken payload.
        var json = KeyValueBagUtil.Serialize(new MessageData().SetStringValue("payload", stored));
        var recovered = KeyValueBagUtil.DeserializeMessageData(json).ToReadable();

        recovered.TryGetJson<TestPayload>("payload").Should().BeNull();
    }

    [Fact]
    public void TryGetJson_WhenKeyMissingOrSetToNull_ReturnsNull()
    {
        var msgData = new MessageData().SetJson<TestPayload>("payload", null).ToReadable();

        msgData.TryGetJson<TestPayload>("payload").Should().BeNull();
        msgData.TryGetJson<TestPayload>("missing").Should().BeNull();
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
