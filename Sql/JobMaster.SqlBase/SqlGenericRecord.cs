namespace JobMaster.SqlBase;

internal class SqlGenericRecordEntry
{
    public string RecordUniqueId { get; set; } = string.Empty;
    public string ClusterId { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public string EntryId { get; set; } = string.Empty;
    public Guid? EntryIdGuid { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? ExpiresAt { get; set; }

    public bool IsReady { get; set; }
    public IList<SqlGenericRecordEntryValue> Values { get; set; } = new List<SqlGenericRecordEntryValue>();
}

internal class SqlGenericRecordEntryValue
{
    public string RecordUniqueId { get; set; } = string.Empty;
    public string KeyName { get; set; } = string.Empty;
    public long? ValueInt64 { get; set; }
    public decimal? ValueDecimal { get; set; }
    public string? ValueText { get; set; }
    public bool? ValueBool { get; set; }
    public DateTime? ValueDateTime { get; set; }
    public Guid? ValueGuid { get; set; }
    public byte[]? ValueBinary { get; set; }
}

internal class SqlGenericRecordEntryLinearDto
{
    public string RecordUniqueId { get; set; } = string.Empty;
    public string ClusterId { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public string EntryId { get; set; } = string.Empty;

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? ExpiresAt { get; set; }
    public bool IsReady { get; set; }

    public string KeyName { get; set; } = string.Empty;

    public long? ValueInt64 { get; set; }
    public decimal? ValueDecimal { get; set; }
    public string? ValueText { get; set; }
    public bool? ValueBool { get; set; }
    public DateTime? ValueDateTime { get; set; }
    public Guid? ValueGuid { get; set; }
    public byte[]? ValueBinary { get; set; }
}
