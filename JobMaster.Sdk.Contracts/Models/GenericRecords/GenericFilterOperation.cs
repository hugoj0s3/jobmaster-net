namespace JobMaster.Sdk.Contracts.Models.GenericRecords;

public enum GenericFilterOperation
{
    Eq,
    Neq,
    In,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,   // text contains substring
    StartsWith, // text starts with
    EndsWith    // text ends with
}