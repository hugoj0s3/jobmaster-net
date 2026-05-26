namespace JobMaster.Api.ApiModels;

/// <summary>Base model for API response types that are scoped to a cluster.</summary>
public class ApiClusterBaseModel
{
    /// <summary>Identifier of the cluster this record belongs to.</summary>
    public string ClusterId { get; set; } = string.Empty;
}