namespace JobMaster.Abstractions.Models.Attributes;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public class JobMasterMetadataAttribute : Attribute
{
    public string Key { get; set; }
    
    public string? StringValue { get; set; }
    public int? IntValue { get; set; }
    public long? LongValue { get; set; }
    public short? ShortValue { get; set; }
    public byte? ByteValue { get; set; }
    public double? DoubleValue { get; set; }
    public decimal? DecimalValue { get; set; }
    public bool? BoolValue { get; set; }
    public char? CharValue { get; set; }
    
    public JobMasterMetadataAttribute(string key, string value)
    {
        Key = key;
        StringValue = value;
    }
    
    public  JobMasterMetadataAttribute(string key, int value)
    {
        Key = key;
        IntValue = value;
    }
    
    public JobMasterMetadataAttribute(string key, long value)
    {
        Key = key;
        LongValue = value;
    }

    public JobMasterMetadataAttribute(string key, short value)
    {
        Key = key;
        ShortValue = value;
    }

    public JobMasterMetadataAttribute(string key, byte value)
    {
        Key = key;
        ByteValue = value;
    }
    
    public JobMasterMetadataAttribute(string key, double value)
    {
        Key = key;
        DoubleValue = value;
    }
    
    public JobMasterMetadataAttribute(string key, decimal value)
    {
        Key = key;
        DecimalValue = value;
    }
    
    public JobMasterMetadataAttribute(string key, bool value)
    {
        Key = key;
        BoolValue = value;
    }

    public JobMasterMetadataAttribute(string key, char value)
    {
        Key = key;
        CharValue = value;
    }
    
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