using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Models.GenericRecords;

[EditorBrowsable(EditorBrowsableState.Never)]
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