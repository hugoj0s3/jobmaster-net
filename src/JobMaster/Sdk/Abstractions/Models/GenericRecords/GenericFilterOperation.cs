namespace JobMaster.Sdk.Abstractions.Models.GenericRecords;

internal enum GenericFilterOperation
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