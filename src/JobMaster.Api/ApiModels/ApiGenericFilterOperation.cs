namespace JobMaster.Api.ApiModels;

/// <summary>Filter comparison operation used in generic record queries.</summary>
public enum ApiGenericFilterOperation
{
    /// <summary>Equal to.</summary>
    Eq,
    /// <summary>Not equal to.</summary>
    Neq,
    /// <summary>Value is contained in the provided list.</summary>
    In,
    /// <summary>Greater than.</summary>
    Gt,
    /// <summary>Greater than or equal to.</summary>
    Gte,
    /// <summary>Less than.</summary>
    Lt,
    /// <summary>Less than or equal to.</summary>
    Lte,
    /// <summary>String contains the given substring.</summary>
    Contains,
    /// <summary>String starts with the given prefix.</summary>
    StartsWith,
    /// <summary>String ends with the given suffix.</summary>
    EndsWith,
}