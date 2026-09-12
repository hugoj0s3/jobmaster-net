namespace JobMaster.Abstractions.Models.Attributes;

/// <summary>
/// Attaches a default metadata key-value pair to all jobs scheduled for this handler.
/// Apply multiple times to set more than one entry.
/// Metadata set at scheduling time is merged with (and takes precedence over) attribute-level metadata.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public class JobMasterMetadataAttribute : Attribute
{
    /// <summary>The metadata key.</summary>
    public string Key { get; set; }

    /// <summary>String value, set when the attribute is constructed with a <see cref="string"/> value.</summary>
    public string? StringValue { get; set; }
    /// <summary>Integer value, set when the attribute is constructed with an <see cref="int"/> value.</summary>
    public int? IntValue { get; set; }
    /// <summary>Long value, set when the attribute is constructed with a <see cref="long"/> value.</summary>
    public long? LongValue { get; set; }
    /// <summary>Short value, set when the attribute is constructed with a <see cref="short"/> value.</summary>
    public short? ShortValue { get; set; }
    /// <summary>Byte value, set when the attribute is constructed with a <see cref="byte"/> value.</summary>
    public byte? ByteValue { get; set; }
    /// <summary>Double value, set when the attribute is constructed with a <see cref="double"/> value.</summary>
    public double? DoubleValue { get; set; }
    /// <summary>Decimal value, set when the attribute is constructed with a <see cref="decimal"/> value.</summary>
    public decimal? DecimalValue { get; set; }
    /// <summary>Boolean value, set when the attribute is constructed with a <see cref="bool"/> value.</summary>
    public bool? BoolValue { get; set; }
    /// <summary>Char value, set when the attribute is constructed with a <see cref="char"/> value.</summary>
    public char? CharValue { get; set; }

    /// <summary>Initializes the attribute with a <see cref="string"/> value.</summary>
    public JobMasterMetadataAttribute(string key, string value)
    {
        Key = key;
        StringValue = value;
    }

    /// <summary>Initializes the attribute with an <see cref="int"/> value.</summary>
    public  JobMasterMetadataAttribute(string key, int value)
    {
        Key = key;
        IntValue = value;
    }

    /// <summary>Initializes the attribute with a <see cref="long"/> value.</summary>
    public JobMasterMetadataAttribute(string key, long value)
    {
        Key = key;
        LongValue = value;
    }

    /// <summary>Initializes the attribute with a <see cref="short"/> value.</summary>
    public JobMasterMetadataAttribute(string key, short value)
    {
        Key = key;
        ShortValue = value;
    }

    /// <summary>Initializes the attribute with a <see cref="byte"/> value.</summary>
    public JobMasterMetadataAttribute(string key, byte value)
    {
        Key = key;
        ByteValue = value;
    }

    /// <summary>Initializes the attribute with a <see cref="double"/> value.</summary>
    public JobMasterMetadataAttribute(string key, double value)
    {
        Key = key;
        DoubleValue = value;
    }

    /// <summary>Initializes the attribute with a <see cref="decimal"/> value.</summary>
    public JobMasterMetadataAttribute(string key, decimal value)
    {
        Key = key;
        DecimalValue = value;
    }

    /// <summary>Initializes the attribute with a <see cref="bool"/> value.</summary>
    public JobMasterMetadataAttribute(string key, bool value)
    {
        Key = key;
        BoolValue = value;
    }

    /// <summary>Initializes the attribute with a <see cref="char"/> value.</summary>
    public JobMasterMetadataAttribute(string key, char value)
    {
        Key = key;
        CharValue = value;
    }

    /// <summary>Returns this attribute's key and the first non-null typed value as a <see cref="KeyValuePair{TKey,TValue}"/>.</summary>
    public KeyValuePair<string, object?> ToKeyValuePair()
    {
        object? objVal =
            (object?)StringValue ??
            (object?)IntValue ??
            (object?)LongValue ??
            (object?)ShortValue ??
            (object?)ByteValue ??
            (object?)DoubleValue ??
            (object?)DecimalValue ??
            (object?)BoolValue ??
            (object?)CharValue;

        return new KeyValuePair<string, object?>(Key, objVal);
    }
}
