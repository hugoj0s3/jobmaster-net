namespace JobMaster.Sdk.Abstractions.Models.GenericRecords;

internal sealed class GenericRecordQueryCriteria
{
    public IList<string> EntryIds { get; set; } = new List<string>();
    
    public string? SubjectType { get; set; }
    public IList<string> SubjectIds { get; set; } = new List<string>();
    
    public bool IncludeExpired { get; set; } = false;
    
    public DateTime? CreatedAtFrom { get; set; }
    public DateTime? CreatedAtTo { get; set; }
    
    public DateTime? ExpiresAtFrom { get; set; }
    public DateTime? ExpiresAtTo { get; set; }
    public IList<GenericRecordValueFilter> Filters { get; set; } = new List<GenericRecordValueFilter>();
    // Optional paging
    public int? Limit { get; set; }
    public int? Offset { get; set; }
    
    public ReadIsolationLevel ReadIsolationLevel { get; set; } = ReadIsolationLevel.Consistent;
    
    public GenericRecordQueryOrderByTypeId OrderBy { get; set; } = GenericRecordQueryOrderByTypeId.CreatedAtDesc;
}

internal enum GenericRecordQueryOrderByTypeId
{
    CreatedAtAsc = 1,
    CreatedAtDesc = 2,
    ExpiresAtAsc = 3,
    ExpiresAtDesc = 4,
}